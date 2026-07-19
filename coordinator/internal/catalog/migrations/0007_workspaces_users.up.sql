CREATE TABLE workspaces (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT UNIQUE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id UUID NOT NULL REFERENCES workspaces(id),
  email TEXT UNIQUE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Single default workspace: auth is groundwork at this stage (JWT
-- middleware + claims flowing into query_history for future attribution),
-- not per-workspace data filtering — see docs/atlas-implementation-spec.md's
-- cross-cutting auth note.
INSERT INTO workspaces (id, name) VALUES
  ('00000000-0000-0000-0000-000000000001', 'default');

ALTER TABLE query_history
  ADD COLUMN workspace_id UUID REFERENCES workspaces(id),
  ADD COLUMN user_id UUID;
