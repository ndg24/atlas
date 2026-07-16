CREATE TABLE snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id UUID NOT NULL REFERENCES datasets(id),
  parent_snapshot_id UUID REFERENCES snapshots(id),
  manifest_list_path TEXT NOT NULL,
  operation TEXT NOT NULL,
  summary_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
