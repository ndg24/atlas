package scheduler_test

// Cross-service integration test (docs/atlas-implementation-spec.md Phase 3,
// task 9): ingests a 3-partition dataset, runs a distributed GROUP BY
// through the full coordinator -> 3 real atlas-worker processes -> merge
// path, and asserts the result exactly matches a hand-computed single-node
// baseline for the same rows — plus that killing one worker doesn't fail the
// query. Spawns real `atlas-worker`/`atlas-cli` binaries (built once via
// `cargo build`) and a real Postgres via testcontainers-go, so it needs both
// a Rust toolchain and Docker; skipped under `go test -short`.

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"atlas/coordinator/internal/catalog"
	catalogpb "atlas/coordinator/internal/catalogpb"
	"atlas/coordinator/internal/scheduler"
)

const distributedGroupBySQL = "SELECT diagnosis, COUNT(*) AS n, SUM(cost) AS total, AVG(cost) AS avg_cost " +
	"FROM t GROUP BY diagnosis ORDER BY diagnosis"

// partitions holds three disjoint row sets. Hand-computed baseline for the
// whole set (9 rows): cold -> ages {20,80,40,10}, cost sum 280, avg 70;
// flu -> ages {60,70,30,90,50}, cost sum 800, avg 160.
var partitions = [][][]string{
	{{"diagnosis", "age", "cost"}, {"flu", "60", "100"}, {"flu", "70", "200"}, {"cold", "20", "50"}},
	{{"diagnosis", "age", "cost"}, {"cold", "80", "150"}, {"flu", "30", "75"}, {"cold", "40", "60"}},
	{{"diagnosis", "age", "cost"}, {"flu", "90", "300"}, {"cold", "10", "20"}, {"flu", "50", "125"}},
}

var wantRows = []groupRow{
	{diagnosis: "cold", count: 4, total: 280, avg: 70},
	{diagnosis: "flu", count: 5, total: 800, avg: 160},
}

type groupRow struct {
	diagnosis string
	count     int64
	total     float64
	avg       float64
}

// --- building the Rust binaries once per test binary run ---

var (
	buildOnce sync.Once
	workerBin string
	cliBin    string
	buildErr  error
)

func ensureBinaries(t *testing.T) (worker, cli string) {
	t.Helper()
	buildOnce.Do(func() {
		root, err := repoRoot()
		if err != nil {
			buildErr = err
			return
		}
		engineDir := filepath.Join(root, "engine")
		cmd := exec.Command("cargo", "build", "-p", "atlas-worker", "-p", "atlas-cli")
		cmd.Dir = engineDir
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("building atlas-worker/atlas-cli: %w\n%s", err, out)
			return
		}

		exeSuffix := ""
		if runtime.GOOS == "windows" {
			exeSuffix = ".exe"
		}
		workerBin = filepath.Join(engineDir, "target", "debug", "atlas-worker"+exeSuffix)
		cliBin = filepath.Join(engineDir, "target", "debug", "atlas-cli"+exeSuffix)
	})
	if buildErr != nil {
		t.Fatalf("building Rust binaries: %v", buildErr)
	}
	return workerBin, cliBin
}

func repoRoot() (string, error) {
	return filepath.Abs(filepath.Join("..", "..", ".."))
}

// --- worker process management ---

type workerProc struct {
	addr string
	cmd  *exec.Cmd
}

func startWorker(t *testing.T, binPath string, port int) *workerProc {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cmd := exec.Command(binPath, "--addr", addr)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting worker at %s: %v", addr, err)
	}
	waitForPort(t, addr)
	return &workerProc{addr: addr, cmd: cmd}
}

func (w *workerProc) kill() {
	if w.cmd.Process != nil {
		_ = w.cmd.Process.Kill()
		_, _ = w.cmd.Process.Wait()
	}
}

func waitForPort(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("worker at %s did not start listening in time", addr)
}

// --- catalog: real Postgres (testcontainers) behind a real gRPC server ---

func startCatalog(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("atlas"),
		tcpostgres.WithUsername("atlas"),
		tcpostgres.WithPassword("atlas"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp")),
	)
	if err != nil {
		t.Fatalf("starting postgres container: %v", err)
	}

	databaseURL, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("getting connection string: %v", err)
	}
	if err := catalog.RunMigrations(databaseURL); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("creating pgx pool: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for catalog server: %v", err)
	}
	grpcServer := grpc.NewServer()
	catalogpb.RegisterCatalogServiceServer(grpcServer, catalog.NewService(pool))
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	addr = lis.Addr().String()
	cleanup = func() {
		grpcServer.GracefulStop()
		pool.Close()
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminating postgres container: %v", err)
		}
	}
	return addr, cleanup
}

// --- fixture: 3 ingested partitions registered as one dataset's snapshot ---

type fixture struct {
	schemaJSON string
	manifests  []scheduler.Manifest
	workers    []*workerProc
	cleanup    func()
}

func writeCSV(t *testing.T, path string, rows [][]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating csv %s: %v", path, err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.WriteAll(rows); err != nil {
		t.Fatalf("writing csv %s: %v", path, err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatalf("flushing csv %s: %v", path, err)
	}
}

// ingestPartition runs `atlas-cli ingest` against a throwaway dataset name so
// the real Rust ingestion path produces a `.atlas` file, then returns that
// file's path discovered on disk (not parsed from stdout).
func ingestPartition(t *testing.T, cliBin, catalogAddr, csvPath, dataDir, dataset string) string {
	t.Helper()
	cmd := exec.Command(cliBin, "ingest",
		"--file", csvPath,
		"--dataset", dataset,
		"--data-dir", dataDir,
		"--catalog-addr", "http://"+catalogAddr,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ingesting %s: %v\n%s", csvPath, err, out)
	}

	matches, err := filepath.Glob(filepath.Join(dataDir, dataset, "*.atlas"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one .atlas file for dataset %s, got %v (err=%v)", dataset, matches, err)
	}
	return matches[0]
}

func setupFixture(t *testing.T) *fixture {
	t.Helper()
	workerBin, cliBin := ensureBinaries(t)
	catalogAddr, stopCatalog := startCatalog(t)

	var workers []*workerProc
	for _, port := range []int{19101, 19102, 19103} {
		workers = append(workers, startWorker(t, workerBin, port))
	}

	tmpDir := t.TempDir()
	var manifests []scheduler.Manifest
	for i, rows := range partitions {
		csvPath := filepath.Join(tmpDir, fmt.Sprintf("part%d.csv", i))
		writeCSV(t, csvPath, rows)
		filePath := ingestPartition(t, cliBin, catalogAddr, csvPath, tmpDir, fmt.Sprintf("part%d", i))
		manifests = append(manifests, scheduler.Manifest{FilePath: filePath})
	}

	catalogConn, err := grpc.NewClient(catalogAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing catalog: %v", err)
	}
	defer catalogConn.Close()
	catalogClient := catalogpb.NewCatalogServiceClient(catalogConn)
	ctx := context.Background()

	// Every partition was inferred from the same columns/types, so any one
	// of them has the schema_json the combined "patients" dataset needs —
	// reused here rather than hand-writing JSON that has to track
	// atlas-format's serde shape.
	part0, err := catalogClient.GetDataset(ctx, &catalogpb.GetDatasetRequest{Name: "part0"})
	if err != nil {
		t.Fatalf("getting part0 dataset: %v", err)
	}

	ds, err := catalogClient.CreateDataset(ctx, &catalogpb.CreateDatasetRequest{
		Name:       "patients",
		SchemaJson: part0.GetSchemaJson(),
	})
	if err != nil {
		t.Fatalf("creating patients dataset: %v", err)
	}

	manifestInputs := make([]*catalogpb.ManifestInput, len(manifests))
	for i, m := range manifests {
		manifestInputs[i] = &catalogpb.ManifestInput{
			FilePath:            m.FilePath,
			PartitionValuesJson: "{}",
			RowCount:            int64(len(partitions[i]) - 1), // minus the header row
			FileSizeBytes:       1,
			ColumnStatsJson:     "{}",
		}
	}
	if _, err := catalogClient.CommitSnapshot(ctx, &catalogpb.CommitSnapshotRequest{
		DatasetId:        ds.GetId(),
		ManifestListPath: tmpDir,
		Operation:        "append",
		SummaryJson:      "{}",
		Manifests:        manifestInputs,
	}); err != nil {
		t.Fatalf("committing patients snapshot: %v", err)
	}

	cleanup := func() {
		for _, w := range workers {
			w.kill()
		}
		stopCatalog()
	}
	return &fixture{schemaJSON: part0.GetSchemaJson(), manifests: manifests, workers: workers, cleanup: cleanup}
}

func (f *fixture) workerAddrs() []string {
	addrs := make([]string, len(f.workers))
	for i, w := range f.workers {
		addrs[i] = w.addr
	}
	return addrs
}

// --- decoding the Arrow IPC result the same way atlas-cli would ---

func decodeRows(t *testing.T, batches [][]byte) []groupRow {
	t.Helper()
	var rows []groupRow
	for _, b := range batches {
		if len(b) == 0 {
			continue
		}
		reader, err := ipc.NewReader(bytes.NewReader(b))
		if err != nil {
			t.Fatalf("opening arrow ipc stream: %v", err)
		}
		for reader.Next() {
			rec := reader.Record()
			schema := rec.Schema()
			diagIdx, countIdx, totalIdx, avgIdx := -1, -1, -1, -1
			for i := 0; i < schema.NumFields(); i++ {
				switch schema.Field(i).Name {
				case "diagnosis":
					diagIdx = i
				case "n":
					countIdx = i
				case "total":
					totalIdx = i
				case "avg_cost":
					avgIdx = i
				}
			}
			if diagIdx == -1 || countIdx == -1 || totalIdx == -1 || avgIdx == -1 {
				t.Fatalf("result batch missing an expected column: %v", schema)
			}
			for r := 0; r < int(rec.NumRows()); r++ {
				rows = append(rows, groupRow{
					diagnosis: rec.Column(diagIdx).(*array.String).Value(r),
					count:     rec.Column(countIdx).(*array.Int64).Value(r),
					total:     rec.Column(totalIdx).(*array.Float64).Value(r),
					avg:       rec.Column(avgIdx).(*array.Float64).Value(r),
				})
			}
		}
		reader.Release()
	}
	return rows
}

func assertRowsEqual(t *testing.T, want, got []groupRow) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("expected %d rows, got %d: %+v", len(want), len(got), got)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].diagnosis < got[j].diagnosis })
	for i := range want {
		w, g := want[i], got[i]
		if w.diagnosis != g.diagnosis || w.count != g.count ||
			math.Abs(w.total-g.total) > 1e-9 || math.Abs(w.avg-g.avg) > 1e-9 {
			t.Fatalf("row %d: want %+v, got %+v", i, w, g)
		}
	}
}

// --- tests ---

func TestDistributedGroupBy_MatchesSingleNodeBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns real worker processes and a Postgres container")
	}
	f := setupFixture(t)
	defer f.cleanup()

	registry, err := scheduler.NewRegistry(f.workerAddrs())
	if err != nil {
		t.Fatalf("creating registry: %v", err)
	}
	defer registry.Close()
	hbCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.StartHeartbeats(hbCtx, 2*time.Second)
	coordinator := &scheduler.Coordinator{Registry: registry}

	result, err := coordinator.RunQuery(context.Background(), distributedGroupBySQL, f.schemaJSON, f.manifests)
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	assertRowsEqual(t, wantRows, decodeRows(t, result.ArrowIPCBatches))
}

func TestDistributedGroupBy_SurvivesAWorkerDying(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns real worker processes and a Postgres container")
	}
	f := setupFixture(t)
	defer f.cleanup()

	registry, err := scheduler.NewRegistry(f.workerAddrs())
	if err != nil {
		t.Fatalf("creating registry: %v", err)
	}
	defer registry.Close()
	// A long heartbeat interval means the registry still considers the
	// worker we're about to kill "alive" when the query starts, so its
	// assigned task's first ExecuteTask attempt genuinely fails at the gRPC
	// transport level — exercising the retry-on-a-different-worker path a
	// worker dying mid-query would hit, deterministically instead of racing
	// a live kill against an in-flight RPC.
	hbCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.StartHeartbeats(hbCtx, time.Minute)
	coordinator := &scheduler.Coordinator{Registry: registry}

	f.workers[0].kill()

	result, err := coordinator.RunQuery(context.Background(), distributedGroupBySQL, f.schemaJSON, f.manifests)
	if err != nil {
		t.Fatalf("RunQuery should have survived one dead worker via retry, got: %v", err)
	}

	assertRowsEqual(t, wantRows, decodeRows(t, result.ArrowIPCBatches))
}
