CREATE TABLE query_history (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT NOT NULL,
  raw_input TEXT NOT NULL,
  logical_plan_json JSONB NOT NULL,
  physical_plan_json JSONB,
  status TEXT NOT NULL,
  duration_ms INTEGER,
  error TEXT
);
