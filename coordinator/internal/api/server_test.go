package api_test

// handleExplain is fully testable without Docker/Postgres/a real Rust
// worker: it never touches query_history (a real *pgxpool.Pool-backed
// history.Store, which would need Postgres), so a fake in-process
// WorkerServiceServer (plain Go, canned Compile response) plus a fake
// CatalogServiceClient (a plain struct satisfying the generated interface,
// no gRPC needed since it's a client-side interface) are enough to exercise
// the compile -> partition-prune -> cache-check path end to end.
// handleQuery, which does write query_history, is covered by the existing
// Docker-gated coordinator/internal/scheduler integration test instead.

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"atlas/coordinator/internal/api"
	"atlas/coordinator/internal/cache"
	catalogpb "atlas/coordinator/internal/catalogpb"
	"atlas/coordinator/internal/scheduler"
	pb "atlas/coordinator/internal/workerpb"
)

func stringsReader(s string) *strings.Reader { return strings.NewReader(s) }

// --- fake catalog client: a plain struct satisfying the generated
// CatalogServiceClient interface directly, no network involved. ---

type fakeCatalog struct {
	schemaJSON string
	snapshotID string
	manifests  []*catalogpb.Manifest
}

func (f *fakeCatalog) CreateDataset(ctx context.Context, in *catalogpb.CreateDatasetRequest, opts ...grpc.CallOption) (*catalogpb.Dataset, error) {
	return nil, nil
}
func (f *fakeCatalog) GetDataset(ctx context.Context, in *catalogpb.GetDatasetRequest, opts ...grpc.CallOption) (*catalogpb.Dataset, error) {
	return &catalogpb.Dataset{Name: in.GetName(), SchemaJson: f.schemaJSON, CurrentSnapshotId: f.snapshotID}, nil
}
func (f *fakeCatalog) ListDatasets(ctx context.Context, in *catalogpb.ListDatasetsRequest, opts ...grpc.CallOption) (*catalogpb.ListDatasetsResponse, error) {
	return &catalogpb.ListDatasetsResponse{}, nil
}
func (f *fakeCatalog) CommitSnapshot(ctx context.Context, in *catalogpb.CommitSnapshotRequest, opts ...grpc.CallOption) (*catalogpb.Snapshot, error) {
	return nil, nil
}
func (f *fakeCatalog) GetCurrentSnapshot(ctx context.Context, in *catalogpb.GetSnapshotRequest, opts ...grpc.CallOption) (*catalogpb.Snapshot, error) {
	return &catalogpb.Snapshot{Id: f.snapshotID}, nil
}
func (f *fakeCatalog) ListManifests(ctx context.Context, in *catalogpb.ListManifestsRequest, opts ...grpc.CallOption) (*catalogpb.ListManifestsResponse, error) {
	return &catalogpb.ListManifestsResponse{Manifests: f.manifests}, nil
}

// --- fake worker: canned Compile response, in-process real gRPC server. ---

type fakeWorker struct {
	pb.UnimplementedWorkerServiceServer
	optimizedPlanJSON string
}

func (w *fakeWorker) Compile(ctx context.Context, req *pb.CompileRequest) (*pb.CompileResponse, error) {
	return &pb.CompileResponse{
		LogicalPlanJson:   w.optimizedPlanJSON,
		OptimizedPlanJson: w.optimizedPlanJSON,
		PartialPlanJson:   w.optimizedPlanJSON,
		NeedsCombine:      false,
	}, nil
}

func (w *fakeWorker) ExecuteTask(req *pb.TaskRequest, stream grpc.ServerStreamingServer[pb.ResultBatch]) error {
	return stream.Send(&pb.ResultBatch{ArrowIpc: []byte("fake-batch")})
}

func (w *fakeWorker) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Alive: true}, nil
}

func startFakeWorker(t *testing.T, optimizedPlanJSON string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for fake worker: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterWorkerServiceServer(srv, &fakeWorker{optimizedPlanJSON: optimizedPlanJSON})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// planWithYearFilter builds a serde-shaped LogicalPlan JSON string:
// Project(Filter(Scan)) filtering on `year = 2024`, matching the real
// atlas_query::LogicalPlan JSON shape (verified against actual Rust output
// during development).
func planWithYearFilter(t *testing.T) string {
	t.Helper()
	scan := map[string]any{"Scan": map[string]any{"dataset": "t", "columns": []string{"diagnosis"}, "snapshot_id": ""}}
	predicate := map[string]any{"Binary": map[string]any{
		"left":  map[string]any{"Column": "year"},
		"op":    "Eq",
		"right": map[string]any{"Literal": map[string]any{"Int": 2024}},
	}}
	filter := map[string]any{"Filter": map[string]any{"input": scan, "predicate": predicate}}
	project := map[string]any{"Project": map[string]any{
		"input": filter, "exprs": []any{map[string]any{"Column": "diagnosis"}}, "aliases": []string{"diagnosis"},
	}}
	b, err := json.Marshal(project)
	if err != nil {
		t.Fatalf("building plan fixture: %v", err)
	}
	return string(b)
}

func manifestWithYear(t *testing.T, filePath string, year int) *catalogpb.Manifest {
	t.Helper()
	pv, err := json.Marshal(map[string]any{"year": year})
	if err != nil {
		t.Fatalf("marshaling partition values: %v", err)
	}
	return &catalogpb.Manifest{FilePath: filePath, PartitionValuesJson: string(pv), ColumnStatsJson: "{}"}
}

func newTestServer(t *testing.T, catalog *fakeCatalog, workerAddr string) (*api.Server, *cache.ResultCache) {
	t.Helper()
	registry, err := scheduler.NewRegistry([]string{workerAddr})
	if err != nil {
		t.Fatalf("creating registry: %v", err)
	}
	t.Cleanup(registry.Close)
	coordinator := &scheduler.Coordinator{Registry: registry}

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("starting miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	resultCache := cache.NewWithClient(rdb, 0)

	return api.NewServer(catalog, coordinator, nil, resultCache), resultCache
}

func TestHandleExplain_PrunesPartitionsAndReportsCacheMiss(t *testing.T) {
	planJSON := planWithYearFilter(t)
	workerAddr := startFakeWorker(t, planJSON)

	catalog := &fakeCatalog{
		schemaJSON: `{"fields":[]}`,
		snapshotID: "snap-1",
		manifests: []*catalogpb.Manifest{
			manifestWithYear(t, "year=2020/part.atlas", 2020),
			manifestWithYear(t, "year=2023/part.atlas", 2023),
			manifestWithYear(t, "year=2024/part.atlas", 2024),
		},
	}
	server, _ := newTestServer(t, catalog, workerAddr)

	body := `{"dataset":"patients","sql":"SELECT diagnosis FROM t WHERE year = 2024"}`
	req := httptest.NewRequest(http.MethodPost, "/explain", stringsReader(body))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		LogicalPlan            json.RawMessage `json:"logical_plan"`
		OptimizedPlan          json.RawMessage `json:"optimized_plan"`
		ManifestsBeforePruning []string        `json:"manifests_before_pruning"`
		ManifestsAfterPruning  []string        `json:"manifests_after_pruning"`
		CacheHit               bool            `json:"cache_hit"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v\nbody: %s", err, rec.Body.String())
	}

	if len(resp.ManifestsBeforePruning) != 3 {
		t.Fatalf("expected 3 manifests before pruning, got %d", len(resp.ManifestsBeforePruning))
	}
	if len(resp.ManifestsAfterPruning) != 1 || resp.ManifestsAfterPruning[0] != "year=2024/part.atlas" {
		t.Fatalf("expected only the year=2024 manifest to survive pruning, got %v", resp.ManifestsAfterPruning)
	}
	if len(resp.LogicalPlan) == 0 || len(resp.OptimizedPlan) == 0 {
		t.Fatal("expected non-empty logical_plan and optimized_plan in the response")
	}
	if resp.CacheHit {
		t.Fatal("expected cache_hit=false — nothing has been cached yet")
	}
}

func TestHandleExplain_ReportsCacheHitOnceEntryExists(t *testing.T) {
	planJSON := planWithYearFilter(t)
	workerAddr := startFakeWorker(t, planJSON)

	catalog := &fakeCatalog{
		schemaJSON: `{"fields":[]}`,
		snapshotID: "snap-1",
		manifests:  []*catalogpb.Manifest{manifestWithYear(t, "year=2024/part.atlas", 2024)},
	}
	server, resultCache := newTestServer(t, catalog, workerAddr)

	// Simulate a prior /query call having populated the cache for this exact
	// (optimized plan, snapshot) pair.
	key := cache.Key(planJSON, "snap-1")
	if err := resultCache.Set(context.Background(), key, "snap-1", [][]byte{[]byte("cached")}); err != nil {
		t.Fatalf("seeding cache: %v", err)
	}

	body := `{"dataset":"patients","sql":"SELECT diagnosis FROM t WHERE year = 2024"}`
	req := httptest.NewRequest(http.MethodPost, "/explain", stringsReader(body))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	var resp struct {
		CacheHit bool `json:"cache_hit"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v\nbody: %s", err, rec.Body.String())
	}
	if !resp.CacheHit {
		t.Fatal("expected cache_hit=true once a matching entry was seeded")
	}
}
