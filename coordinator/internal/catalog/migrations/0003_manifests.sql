CREATE TABLE manifests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_id UUID NOT NULL REFERENCES snapshots(id),
  file_path TEXT NOT NULL,
  partition_values JSONB,
  row_count BIGINT NOT NULL,
  file_size_bytes BIGINT NOT NULL,
  column_stats JSONB NOT NULL
);
