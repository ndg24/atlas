// Package api implements the coordinator's REST surface
// (docs/atlas-implementation-spec.md Phase 3 §1.4): POST /query fans a SQL
// query out across workers and merges the result, POST /datasets and
// GET /datasets proxy the catalog, and GET /history reads query_history.
//
// The coordinator never decodes the Arrow IPC bytes flowing through it —
// /query's response carries them base64-encoded, opaque, exactly as
// produced by the workers — so callers that understand Arrow (atlas-cli)
// decode them, and the coordinator stays a pure bytes-shuffling orchestrator.
package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	catalogpb "atlas/coordinator/internal/catalogpb"
	"atlas/coordinator/internal/history"
	"atlas/coordinator/internal/scheduler"
)

type Server struct {
	catalog     catalogpb.CatalogServiceClient
	coordinator *scheduler.Coordinator
	history     *history.Store
}

func NewServer(catalog catalogpb.CatalogServiceClient, coordinator *scheduler.Coordinator, historyStore *history.Store) *Server {
	return &Server{catalog: catalog, coordinator: coordinator, history: historyStore}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /query", s.handleQuery)
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

type queryRequest struct {
	Dataset string `json:"dataset"`
	SQL     string `json:"sql"`
}

type queryResponse struct {
	QueryID    string `json:"query_id"`
	DurationMs int64  `json:"duration_ms"`
	// Each entry is one self-contained, base64-encoded Arrow IPC stream.
	ArrowIPCBatches []string `json:"arrow_ipc_batches"`
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

	ds, err := s.catalog.GetDataset(ctx, &catalogpb.GetDatasetRequest{Name: req.Dataset})
	if err != nil {
		fail(fmt.Errorf("looking up dataset %q: %w", req.Dataset, err))
		return
	}
	if ds.GetCurrentSnapshotId() == "" {
		fail(fmt.Errorf("dataset %q has no committed snapshot yet — run ingest first", req.Dataset))
		return
	}

	snapshot, err := s.catalog.GetCurrentSnapshot(ctx, &catalogpb.GetSnapshotRequest{DatasetName: req.Dataset})
	if err != nil {
		fail(fmt.Errorf("fetching current snapshot: %w", err))
		return
	}
	manifestResp, err := s.catalog.ListManifests(ctx, &catalogpb.ListManifestsRequest{SnapshotId: snapshot.GetId()})
	if err != nil {
		fail(fmt.Errorf("listing manifests: %w", err))
		return
	}
	if len(manifestResp.GetManifests()) == 0 {
		fail(fmt.Errorf("dataset %q snapshot has no manifests", req.Dataset))
		return
	}
	manifests := make([]scheduler.Manifest, len(manifestResp.GetManifests()))
	for i, m := range manifestResp.GetManifests() {
		manifests[i] = scheduler.Manifest{FilePath: m.GetFilePath()}
	}

	compiled, err := s.coordinator.Compile(ctx, req.SQL, ds.GetSchemaJson())
	if err != nil {
		fail(fmt.Errorf("compiling query: %w", err))
		return
	}
	if err := s.history.SetPlan(ctx, historyID, compiled.GetPartialPlanJson()); err != nil {
		fail(err)
		return
	}

	result, err := s.coordinator.RunCompiled(ctx, compiled, manifests)
	if err != nil {
		fail(err)
		return
	}

	durationMs := int(time.Since(started).Milliseconds())
	if err := s.history.Finish(ctx, historyID, "succeeded", durationMs, ""); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("query succeeded but recording history failed: %w", err))
		return
	}

	batches := make([]string, len(result.ArrowIPCBatches))
	for i, b := range result.ArrowIPCBatches {
		batches[i] = base64.StdEncoding.EncodeToString(b)
	}
	writeJSON(w, http.StatusOK, queryResponse{
		QueryID:         historyID,
		DurationMs:      int64(durationMs),
		ArrowIPCBatches: batches,
	})
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
