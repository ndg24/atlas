CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE datasets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT UNIQUE NOT NULL,
  schema_json JSONB NOT NULL,
  current_snapshot_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
