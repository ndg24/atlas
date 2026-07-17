package catalog_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"atlas/coordinator/internal/catalog"
	pb "atlas/coordinator/internal/catalogpb"
)

func newTestService(t *testing.T) *catalog.Service {
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

	return catalog.NewService(pool)
}

func TestCommitSnapshot_ChainsParentAcrossTwoCommits(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

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
	svc := newTestService(t)

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
