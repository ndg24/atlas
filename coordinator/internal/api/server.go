// Package api implements the coordinator's REST surface
// (docs/atlas-implementation-spec.md Phase 3 §1.4, Phase 4): POST /query
// fans a SQL query out across workers and merges the result, POST /explain
// runs the same compile+prune steps as a dry run (no task dispatch, no
// cache write, no history row), POST /datasets and GET /datasets proxy the
// catalog, and GET /history reads query_history.
//
// The coordinator never decodes the Arrow IPC bytes flowing through it —
// /query's response carries them base64-encoded, opaque, exactly as
// produced by the workers — so callers that understand Arrow (atlas-cli)
// decode them, and the coordinator stays a pure bytes-shuffling orchestrator.
// The one exception, added in Phase 4, is `planjson`: a narrow decoder for
// exactly two plan fields (Scan columns, top Filter predicate) needed for
// column/partition pruning — not general plan-awareness.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"atlas/coordinator/internal/cache"
	catalogpb "atlas/coordinator/internal/catalogpb"
	"atlas/coordinator/internal/history"
	"atlas/coordinator/internal/planjson"
	"atlas/coordinator/internal/scheduler"
)

type Server struct {
	catalog     catalogpb.CatalogServiceClient
	coordinator *scheduler.Coordinator
	history     *history.Store
	cache       *cache.ResultCache
}

// NewServer wires up the REST API. resultCache may be nil (caching disabled)
// — every cache access below is guarded accordingly.
func NewServer(catalog catalogpb.CatalogServiceClient, coordinator *scheduler.Coordinator, historyStore *history.Store, resultCache *cache.ResultCache) *Server {
	return &Server{catalog: catalog, coordinator: coordinator, history: historyStore, cache: resultCache}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /query", s.handleQuery)
	mux.HandleFunc("POST /explain", s.handleExplain)
	mux.HandleFunc("POST /datasets", s.handleCreateDataset)
	mux.HandleFunc("GET /datasets", s.handleListDatasets)
	mux.HandleFunc("GET /history", s.handleHistory)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// datasetQuery is a dataset's current snapshot id and manifest list — the
// state both /query and /explain need before compiling anything.
type datasetQuery struct {
	dataset    *catalogpb.Dataset
	snapshotID string
	manifests  []*catalogpb.Manifest
}

// lookupDataset resolves name to its current snapshot and manifest list.
// Returns a plain error (never writes to the response) so /query can fold a
// failure into the query_history row it already has in flight, while
// /explain (which has no such row) can just write the error directly.
func (s *Server) lookupDataset(ctx context.Context, name string) (*datasetQuery, error) {
	ds, err := s.catalog.GetDataset(ctx, &catalogpb.GetDatasetRequest{Name: name})
	if err != nil {
		return nil, fmt.Errorf("looking up dataset %q: %w", name, err)
	}
	if ds.GetCurrentSnapshotId() == "" {
		return nil, fmt.Errorf("dataset %q has no committed snapshot yet — run ingest first", name)
	}

	snapshot, err := s.catalog.GetCurrentSnapshot(ctx, &catalogpb.GetSnapshotRequest{DatasetName: name})
	if err != nil {
		return nil, fmt.Errorf("fetching current snapshot: %w", err)
	}
	manifestResp, err := s.catalog.ListManifests(ctx, &catalogpb.ListManifestsRequest{SnapshotId: snapshot.GetId()})
	if err != nil {
		return nil, fmt.Errorf("listing manifests: %w", err)
	}
	if len(manifestResp.GetManifests()) == 0 {
		return nil, fmt.Errorf("dataset %q snapshot has no manifests", name)
	}

	return &datasetQuery{dataset: ds, snapshotID: snapshot.GetId(), manifests: manifestResp.GetManifests()}, nil
}

// pruneManifests applies Phase 4 partition pruning against optimizedPlanJSON's
// top Filter predicate (if any), returning the surviving manifests. Falls
// back to returning every manifest unpruned if there's no filter, or the
// plan JSON can't be parsed (never fails the query over an optimization
// detail).
func pruneManifests(optimizedPlanJSON string, manifests []*catalogpb.Manifest) []*catalogpb.Manifest {
	predicate, ok, err := planjson.ExtractFilterPredicate(optimizedPlanJSON)
	if err != nil || !ok {
		return manifests
	}
	return scheduler.PrunePartitions(predicate, manifests)
}

func manifestFilePaths(manifests []*catalogpb.Manifest) []string {
	paths := make([]string, len(manifests))
	for i, m := range manifests {
		paths[i] = m.GetFilePath()
	}
	return paths
}

func toSchedulerManifests(manifests []*catalogpb.Manifest) []scheduler.Manifest {
	out := make([]scheduler.Manifest, len(manifests))
	for i, m := range manifests {
		out[i] = scheduler.Manifest{FilePath: m.GetFilePath(), Format: m.GetFormat()}
	}
	return out
}

func encodeBatches(batches [][]byte) []string {
	out := make([]string, len(batches))
	for i, b := range batches {
		out[i] = base64.StdEncoding.EncodeToString(b)
	}
	return out
}

type queryRequest struct {
	Dataset string `json:"dataset"`
	SQL     string `json:"sql"`
}

type queryResponse struct {
	QueryID    string `json:"query_id"`
	DurationMs int64  `json:"duration_ms"`
	// Each entry is one self-contained, base64-encoded Arrow IPC stream.
	ArrowIPCBatches []string `json:"arrow_ipc_batches"`
	CacheHit        bool     `json:"cache_hit"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decoding request body: %w", err))
		return
	}
	if req.Dataset == "" || req.SQL == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf(`both "dataset" and "sql" are required`))
		return
	}

	ctx := r.Context()
	started := time.Now()

	historyID, err := s.history.Start(ctx, "sql", req.SQL, "{}")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	fail := func(err error) {
		_ = s.history.Finish(ctx, historyID, "failed", int(time.Since(started).Milliseconds()), err.Error())
		writeError(w, http.StatusInternalServerError, err)
	}

	dq, err := s.lookupDataset(ctx, req.Dataset)
	if err != nil {
		fail(err)
		return
	}

	compiled, err := s.coordinator.Compile(ctx, req.SQL, dq.dataset.GetSchemaJson())
	if err != nil {
		fail(fmt.Errorf("compiling query: %w", err))
		return
	}
	physicalPlanJSON, _ := json.Marshal(map[string]string{
		"partial": compiled.GetPartialPlanJson(), "combine": compiled.GetCombinePlanJson(),
	})
	if err := s.history.SetPlan(ctx, historyID, compiled.GetLogicalPlanJson(), string(physicalPlanJSON)); err != nil {
		fail(err)
		return
	}

	cacheKey := cache.Key(compiled.GetOptimizedPlanJson(), dq.snapshotID)
	if s.cache != nil {
		if entry, hit, cacheErr := s.cache.Get(ctx, cacheKey, dq.snapshotID); cacheErr == nil && hit {
			durationMs := int(time.Since(started).Milliseconds())
			if err := s.history.Finish(ctx, historyID, "succeeded", durationMs, ""); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Errorf("query succeeded (cache hit) but recording history failed: %w", err))
				return
			}
			writeJSON(w, http.StatusOK, queryResponse{
				QueryID:         historyID,
				DurationMs:      int64(durationMs),
				ArrowIPCBatches: encodeBatches(entry.ArrowIPCBatches),
				CacheHit:        true,
			})
			return
		}
	}

	prunedManifests := pruneManifests(compiled.GetOptimizedPlanJson(), dq.manifests)
	result, err := s.coordinator.RunCompiled(ctx, compiled, toSchedulerManifests(prunedManifests))
	if err != nil {
		fail(err)
		return
	}

	durationMs := int(time.Since(started).Milliseconds())
	if err := s.history.Finish(ctx, historyID, "succeeded", durationMs, ""); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("query succeeded but recording history failed: %w", err))
		return
	}

	if s.cache != nil {
		_ = s.cache.Set(ctx, cacheKey, dq.snapshotID, result.ArrowIPCBatches)
	}

	writeJSON(w, http.StatusOK, queryResponse{
		QueryID:         historyID,
		DurationMs:      int64(durationMs),
		ArrowIPCBatches: encodeBatches(result.ArrowIPCBatches),
		CacheHit:        false,
	})
}

type explainRequest struct {
	Dataset string `json:"dataset"`
	SQL     string `json:"sql"`
}

type explainResponse struct {
	LogicalPlan            json.RawMessage `json:"logical_plan"`
	OptimizedPlan          json.RawMessage `json:"optimized_plan"`
	ManifestsBeforePruning []string        `json:"manifests_before_pruning"`
	ManifestsAfterPruning  []string        `json:"manifests_after_pruning"`
	CacheHit               bool            `json:"cache_hit"`
}

// handleExplain is a dry run: it compiles and prunes exactly like /query,
// but never dispatches a task, writes to the cache, or records history — a
// diagnostic view of what /query with the same body *would* do.
func (s *Server) handleExplain(w http.ResponseWriter, r *http.Request) {
	var req explainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decoding request body: %w", err))
		return
	}
	if req.Dataset == "" || req.SQL == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf(`both "dataset" and "sql" are required`))
		return
	}

	ctx := r.Context()
	dq, err := s.lookupDataset(ctx, req.Dataset)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	compiled, err := s.coordinator.Compile(ctx, req.SQL, dq.dataset.GetSchemaJson())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("compiling query: %w", err))
		return
	}

	prunedManifests := pruneManifests(compiled.GetOptimizedPlanJson(), dq.manifests)

	cacheHit := false
	if s.cache != nil {
		key := cache.Key(compiled.GetOptimizedPlanJson(), dq.snapshotID)
		if _, hit, cacheErr := s.cache.Get(ctx, key, dq.snapshotID); cacheErr == nil {
			cacheHit = hit
		}
	}

	writeJSON(w, http.StatusOK, explainResponse{
		LogicalPlan:            json.RawMessage(orNullJSON(compiled.GetLogicalPlanJson())),
		OptimizedPlan:          json.RawMessage(orNullJSON(compiled.GetOptimizedPlanJson())),
		ManifestsBeforePruning: manifestFilePaths(dq.manifests),
		ManifestsAfterPruning:  manifestFilePaths(prunedManifests),
		CacheHit:               cacheHit,
	})
}

func orNullJSON(s string) string {
	if s == "" {
		return "null"
	}
	return s
}

func (s *Server) handleCreateDataset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string `json:"name"`
		SchemaJSON string `json:"schema_json"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ds, err := s.catalog.CreateDataset(r.Context(), &catalogpb.CreateDatasetRequest{Name: req.Name, SchemaJson: req.SchemaJSON})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, ds)
}

func (s *Server) handleListDatasets(w http.ResponseWriter, r *http.Request) {
	resp, err := s.catalog.ListDatasets(r.Context(), &catalogpb.ListDatasetsRequest{})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, resp.GetDatasets())
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	entries, err := s.history.List(r.Context(), 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, entries)
}
