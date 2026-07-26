-- Literature retrieval storage for the Phase 8 research pipeline
-- (ai-service/atlas_ai/retrieval): chunked document text + embeddings, held
-- in the existing Postgres instance behind the pgvector extension rather
-- than standing up a second datastore (docs/atlas-implementation-spec.md
-- Phase 8, task 4). The ai-service connects to this same Postgres via
-- DATABASE_URL and reads/writes this table directly -- it isn't fronted by
-- CatalogService, since corpus documents aren't dataset/snapshot metadata.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE corpus_documents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  corpus_id TEXT NOT NULL,
  doc_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL DEFAULT 0,
  text TEXT NOT NULL,
  -- 768 dims matches the default embedding model (ollama/nomic-embed-text,
  -- see ai-service/atlas_ai/config.py). Switching ATLAS_EMBEDDING_MODEL to a
  -- model with different output dimensions requires a matching migration.
  embedding vector(768) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (corpus_id, doc_id, chunk_index)
);

CREATE INDEX corpus_documents_corpus_id_idx ON corpus_documents (corpus_id);

-- No ivfflat/approximate index: at the fixture/small-corpus scale this
-- slice targets, exact nearest-neighbor via `<=>` (cosine distance) ORDER
-- BY + LIMIT in store.py is simpler and exactly correct. Revisit with an
-- approximate index if a corpus grows large enough for a full scan to
-- matter -- an ivfflat index built on a near-empty table clusters poorly
-- anyway, so it wouldn't help at this scale.
