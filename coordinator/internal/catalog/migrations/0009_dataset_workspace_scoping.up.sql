-- Enforces the workspace scoping migration 0007 laid the groundwork for
-- (workspaces/users/query_history carried workspace_id, but datasets itself
-- never did, so no catalog read/write could actually be filtered by it).
-- Defaulting to the seeded default workspace keeps every dataset ingested
-- before this migration (or by a caller that doesn't pass --workspace-id)
-- attached to it, so this is non-breaking.
ALTER TABLE datasets
  ADD COLUMN workspace_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'
    REFERENCES workspaces(id);
