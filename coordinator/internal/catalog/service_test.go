package catalog_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"atlas/coordinator/internal/catalog"
	pb "atlas/coordinator/internal/catalogpb"
)

// newTestService returns a Service backed by a fresh, migrated Postgres
// container, plus the raw pool — needed by tests (like the workspace-
// scoping one below) that must set up state Service itself has no RPC for,
// e.g. inserting a second workspace row.
func newTestService(t *testing.T) (*catalog.Service, *pgxpool.Pool) {
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
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminating postgres container: %v", err)
		}
	})

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
	t.Cleanup(pool.Close)

	return catalog.NewService(pool), pool
}

func TestCommitSnapshot_ChainsParentAcrossTwoCommits(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)

	ds, err := svc.CreateDataset(ctx, &pb.CreateDatasetRequest{
		Name:       "patients",
		SchemaJson: `{"fields":[]}`,
	})
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}

	first, err := svc.CommitSnapshot(ctx, &pb.CommitSnapshotRequest{
		DatasetId:        ds.GetId(),
		ManifestListPath: "data/patients",
		Operation:        "append",
		SummaryJson:      `{"row_count":10}`,
		Manifests: []*pb.ManifestInput{{
			FilePath:            "data/patients/part-0.atlas",
			PartitionValuesJson: "{}",
			RowCount:            10,
			FileSizeBytes:       1024,
			ColumnStatsJson:     "{}",
		}},
	})
	if err != nil {
		t.Fatalf("CommitSnapshot (first): %v", err)
	}
	if first.GetParentSnapshotId() != "" {
		t.Fatalf("first snapshot should have no parent, got %q", first.GetParentSnapshotId())
	}

	current, err := svc.GetCurrentSnapshot(ctx, &pb.GetSnapshotRequest{DatasetName: "patients"})
	if err != nil {
		t.Fatalf("GetCurrentSnapshot after first commit: %v", err)
	}
	if current.GetId() != first.GetId() {
		t.Fatalf("current snapshot = %q, want %q", current.GetId(), first.GetId())
	}

	second, err := svc.CommitSnapshot(ctx, &pb.CommitSnapshotRequest{
		DatasetId:        ds.GetId(),
		ManifestListPath: "data/patients",
		Operation:        "append",
		SummaryJson:      `{"row_count":5}`,
		Manifests: []*pb.ManifestInput{{
			FilePath:            "data/patients/part-1.atlas",
			PartitionValuesJson: "{}",
			RowCount:            5,
			FileSizeBytes:       512,
			ColumnStatsJson:     "{}",
		}},
	})
	if err != nil {
		t.Fatalf("CommitSnapshot (second): %v", err)
	}
	if second.GetParentSnapshotId() != first.GetId() {
		t.Fatalf("second snapshot's parent = %q, want %q", second.GetParentSnapshotId(), first.GetId())
	}

	current, err = svc.GetCurrentSnapshot(ctx, &pb.GetSnapshotRequest{DatasetName: "patients"})
	if err != nil {
		t.Fatalf("GetCurrentSnapshot after second commit: %v", err)
	}
	if current.GetId() != second.GetId() {
		t.Fatalf("current snapshot = %q, want %q", current.GetId(), second.GetId())
	}

	manifests, err := svc.ListManifests(ctx, &pb.ListManifestsRequest{SnapshotId: second.GetId()})
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(manifests.GetManifests()) != 1 || manifests.GetManifests()[0].GetFilePath() != "data/patients/part-1.atlas" {
		t.Fatalf("unexpected manifests for second snapshot: %+v", manifests.GetManifests())
	}
}

func TestCommitSnapshot_PreservesManifestFormat(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)

	ds, err := svc.CreateDataset(ctx, &pb.CreateDatasetRequest{
		Name:       "patients",
		SchemaJson: `{"fields":[]}`,
	})
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}

	snap, err := svc.CommitSnapshot(ctx, &pb.CommitSnapshotRequest{
		DatasetId:        ds.GetId(),
		ManifestListPath: "data/patients",
		Operation:        "append",
		SummaryJson:      `{"row_count":15}`,
		Manifests: []*pb.ManifestInput{
			{
				FilePath:            "data/patients/part-0.atlas",
				PartitionValuesJson: "{}",
				RowCount:            10,
				FileSizeBytes:       1024,
				ColumnStatsJson:     "{}",
				Format:              "atlas",
			},
			{
				FilePath:            "data/patients/part-1.parquet",
				PartitionValuesJson: "{}",
				RowCount:            5,
				FileSizeBytes:       512,
				ColumnStatsJson:     "{}",
				Format:              "parquet",
			},
			{
				// Older/omitted-format callers should default to "atlas".
				FilePath:            "data/patients/part-2.atlas",
				PartitionValuesJson: "{}",
				RowCount:            3,
				FileSizeBytes:       256,
				ColumnStatsJson:     "{}",
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitSnapshot: %v", err)
	}

	manifests, err := svc.ListManifests(ctx, &pb.ListManifestsRequest{SnapshotId: snap.GetId()})
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}

	formatByPath := map[string]string{}
	for _, m := range manifests.GetManifests() {
		formatByPath[m.GetFilePath()] = m.GetFormat()
	}
	want := map[string]string{
		"data/patients/part-0.atlas":   "atlas",
		"data/patients/part-1.parquet": "parquet",
		"data/patients/part-2.atlas":   "atlas",
	}
	for path, wantFormat := range want {
		if got := formatByPath[path]; got != wantFormat {
			t.Fatalf("manifest %s format = %q, want %q", path, got, wantFormat)
		}
	}
}

// TestDatasetWorkspaceScoping_IsolatesAcrossWorkspaces proves the migration
// 0009 groundwork is actually enforced: a dataset created in one workspace
// is invisible (via GetDataset, GetCurrentSnapshot, and ListDatasets) to a
// request scoped to a different workspace, and visible to one scoped to its
// own workspace.
func TestDatasetWorkspaceScoping_IsolatesAcrossWorkspaces(t *testing.T) {
	ctx := context.Background()
	svc, pool := newTestService(t)

	const workspaceA = "00000000-0000-0000-0000-000000000001" // seeded default, see migration 0007
	var workspaceB string
	if err := pool.QueryRow(ctx,
		`INSERT INTO workspaces (name) VALUES ('other') RETURNING id::text`,
	).Scan(&workspaceB); err != nil {
		t.Fatalf("creating second workspace: %v", err)
	}

	dsA, err := svc.CreateDataset(ctx, &pb.CreateDatasetRequest{
		Name:        "patients_a",
		SchemaJson:  `{"fields":[]}`,
		WorkspaceId: workspaceA,
	})
	if err != nil {
		t.Fatalf("CreateDataset (workspace A): %v", err)
	}
	if _, err := svc.CommitSnapshot(ctx, &pb.CommitSnapshotRequest{
		DatasetId:        dsA.GetId(),
		ManifestListPath: "data/patients_a",
		Operation:        "append",
		SummaryJson:      `{"row_count":1}`,
		Manifests: []*pb.ManifestInput{{
			FilePath:            "data/patients_a/part-0.atlas",
			PartitionValuesJson: "{}",
			RowCount:            1,
			FileSizeBytes:       10,
			ColumnStatsJson:     "{}",
		}},
	}); err != nil {
		t.Fatalf("CommitSnapshot (workspace A): %v", err)
	}

	if _, err := svc.GetDataset(ctx, &pb.GetDatasetRequest{Name: "patients_a", WorkspaceId: workspaceB}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetDataset from workspace B: got err %v, want NotFound", err)
	}
	if _, err := svc.GetCurrentSnapshot(ctx, &pb.GetSnapshotRequest{DatasetName: "patients_a", WorkspaceId: workspaceB}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetCurrentSnapshot from workspace B: got err %v, want NotFound", err)
	}
	listB, err := svc.ListDatasets(ctx, &pb.ListDatasetsRequest{WorkspaceId: workspaceB})
	if err != nil {
		t.Fatalf("ListDatasets (workspace B): %v", err)
	}
	for _, ds := range listB.GetDatasets() {
		if ds.GetName() == "patients_a" {
			t.Fatalf("workspace B's dataset list leaked workspace A's dataset: %+v", listB.GetDatasets())
		}
	}

	if _, err := svc.GetDataset(ctx, &pb.GetDatasetRequest{Name: "patients_a", WorkspaceId: workspaceA}); err != nil {
		t.Fatalf("GetDataset from workspace A: %v", err)
	}
	if _, err := svc.GetCurrentSnapshot(ctx, &pb.GetSnapshotRequest{DatasetName: "patients_a", WorkspaceId: workspaceA}); err != nil {
		t.Fatalf("GetCurrentSnapshot from workspace A: %v", err)
	}
	listA, err := svc.ListDatasets(ctx, &pb.ListDatasetsRequest{WorkspaceId: workspaceA})
	if err != nil {
		t.Fatalf("ListDatasets (workspace A): %v", err)
	}
	found := false
	for _, ds := range listA.GetDatasets() {
		if ds.GetName() == "patients_a" {
			found = true
		}
	}
	if !found {
		t.Fatalf("workspace A's dataset list should include patients_a: %+v", listA.GetDatasets())
	}
}
