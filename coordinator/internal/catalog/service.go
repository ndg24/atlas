// Package catalog implements CatalogService (proto/catalog.proto): the
// Postgres-backed metadata catalog. CommitSnapshot is the one RPC with a
// correctness requirement beyond "does the right query": it must never leave
// a dataset pointing at a partially-written snapshot, so it runs entirely in
// one transaction.
package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "atlas/coordinator/internal/catalogpb"
)

type Service struct {
	pb.UnimplementedCatalogServiceServer
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

const datasetColumns = `id::text, name, schema_json::text, coalesce(current_snapshot_id::text, ''), created_at::text`

func scanDataset(row pgx.Row) (*pb.Dataset, error) {
	var ds pb.Dataset
	err := row.Scan(&ds.Id, &ds.Name, &ds.SchemaJson, &ds.CurrentSnapshotId, &ds.CreatedAt)
	return &ds, err
}

func (s *Service) CreateDataset(ctx context.Context, req *pb.CreateDatasetRequest) (*pb.Dataset, error) {
	ds, err := scanDataset(s.pool.QueryRow(ctx,
		`INSERT INTO datasets (name, schema_json) VALUES ($1, $2) RETURNING `+datasetColumns,
		req.GetName(), req.GetSchemaJson(),
	))
	if err != nil {
		return nil, fmt.Errorf("creating dataset %q: %w", req.GetName(), err)
	}
	return ds, nil
}

func (s *Service) GetDataset(ctx context.Context, req *pb.GetDatasetRequest) (*pb.Dataset, error) {
	ds, err := scanDataset(s.pool.QueryRow(ctx,
		`SELECT `+datasetColumns+` FROM datasets WHERE name = $1`, req.GetName(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "dataset %q not found", req.GetName())
	}
	if err != nil {
		return nil, fmt.Errorf("getting dataset %q: %w", req.GetName(), err)
	}
	return ds, nil
}

func (s *Service) ListDatasets(ctx context.Context, _ *pb.ListDatasetsRequest) (*pb.ListDatasetsResponse, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+datasetColumns+` FROM datasets ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("listing datasets: %w", err)
	}
	defer rows.Close()

	var out []*pb.Dataset
	for rows.Next() {
		ds, err := scanDataset(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning dataset row: %w", err)
		}
		out = append(out, ds)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating dataset rows: %w", err)
	}
	return &pb.ListDatasetsResponse{Datasets: out}, nil
}

// CommitSnapshot inserts the new snapshot row, inserts every manifest row,
// and advances datasets.current_snapshot_id — all in one transaction, so a
// crash mid-commit leaves the catalog at the previous, fully-committed
// snapshot rather than a half-written one.
func (s *Service) CommitSnapshot(ctx context.Context, req *pb.CommitSnapshotRequest) (*pb.Snapshot, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning commit-snapshot transaction: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once Commit has succeeded

	var parentID *string
	if err := tx.QueryRow(ctx,
		`SELECT current_snapshot_id::text FROM datasets WHERE id = $1`, req.GetDatasetId(),
	).Scan(&parentID); err != nil {
		return nil, fmt.Errorf("looking up dataset %s: %w", req.GetDatasetId(), err)
	}

	var snap pb.Snapshot
	err = tx.QueryRow(ctx,
		`INSERT INTO snapshots (dataset_id, parent_snapshot_id, manifest_list_path, operation, summary_json)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id::text, dataset_id::text, coalesce(parent_snapshot_id::text, ''),
		           manifest_list_path, operation, coalesce(summary_json::text, ''), created_at::text`,
		req.GetDatasetId(), parentID, req.GetManifestListPath(), req.GetOperation(), req.GetSummaryJson(),
	).Scan(&snap.Id, &snap.DatasetId, &snap.ParentSnapshotId, &snap.ManifestListPath,
		&snap.Operation, &snap.SummaryJson, &snap.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("inserting snapshot for dataset %s: %w", req.GetDatasetId(), err)
	}

	for _, m := range req.GetManifests() {
		format := m.GetFormat()
		if format == "" {
			format = "atlas"
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO manifests (snapshot_id, file_path, partition_values, row_count, file_size_bytes, column_stats, format)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			snap.Id, m.GetFilePath(), m.GetPartitionValuesJson(), m.GetRowCount(),
			m.GetFileSizeBytes(), m.GetColumnStatsJson(), format,
		); err != nil {
			return nil, fmt.Errorf("inserting manifest %s: %w", m.GetFilePath(), err)
		}
	}

	if _, err := tx.Exec(ctx,
		`UPDATE datasets SET current_snapshot_id = $1 WHERE id = $2`, snap.Id, req.GetDatasetId(),
	); err != nil {
		return nil, fmt.Errorf("advancing current_snapshot_id for dataset %s: %w", req.GetDatasetId(), err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing snapshot transaction: %w", err)
	}
	return &snap, nil
}

func (s *Service) GetCurrentSnapshot(ctx context.Context, req *pb.GetSnapshotRequest) (*pb.Snapshot, error) {
	var snap pb.Snapshot
	err := s.pool.QueryRow(ctx,
		`SELECT s.id::text, s.dataset_id::text, coalesce(s.parent_snapshot_id::text, ''),
		        s.manifest_list_path, s.operation, coalesce(s.summary_json::text, ''), s.created_at::text
		 FROM snapshots s JOIN datasets d ON d.current_snapshot_id = s.id
		 WHERE d.name = $1`,
		req.GetDatasetName(),
	).Scan(&snap.Id, &snap.DatasetId, &snap.ParentSnapshotId, &snap.ManifestListPath,
		&snap.Operation, &snap.SummaryJson, &snap.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "dataset %q has no current snapshot", req.GetDatasetName())
	}
	if err != nil {
		return nil, fmt.Errorf("getting current snapshot for dataset %q: %w", req.GetDatasetName(), err)
	}
	return &snap, nil
}

func (s *Service) ListManifests(ctx context.Context, req *pb.ListManifestsRequest) (*pb.ListManifestsResponse, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id::text, snapshot_id::text, file_path, coalesce(partition_values::text, ''),
		        row_count, file_size_bytes, column_stats::text, format
		 FROM manifests WHERE snapshot_id = $1`,
		req.GetSnapshotId(),
	)
	if err != nil {
		return nil, fmt.Errorf("listing manifests for snapshot %s: %w", req.GetSnapshotId(), err)
	}
	defer rows.Close()

	var out []*pb.Manifest
	for rows.Next() {
		var m pb.Manifest
		if err := rows.Scan(&m.Id, &m.SnapshotId, &m.FilePath, &m.PartitionValuesJson,
			&m.RowCount, &m.FileSizeBytes, &m.ColumnStatsJson, &m.Format); err != nil {
			return nil, fmt.Errorf("scanning manifest row: %w", err)
		}
		out = append(out, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating manifest rows: %w", err)
	}
	return &pb.ListManifestsResponse{Manifests: out}, nil
}
