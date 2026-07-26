"""ingest_documents: chunk + embed + store documents into a corpus
(docs/atlas-implementation-spec.md Phase 8, task 4) -- the write side of
retrieval. Called by ingest_cli.py for real corpus loading and directly by
tests exercising a real Postgres+pgvector instance.
"""

from __future__ import annotations

from .chunking import chunk_text
from .embeddings import EmbeddingProvider
from .store import PgVectorStore


def ingest_documents(
    corpus_id: str,
    documents: list[tuple[str, str]],
    database_url: str,
    embedding_provider: EmbeddingProvider,
) -> int:
    """documents: (doc_id, text) pairs. Returns the total number of chunks
    stored across all documents. Re-ingesting a doc_id already in the corpus
    replaces its chunks (see PgVectorStore.upsert_chunks)."""
    store = PgVectorStore(database_url)
    total = 0
    for doc_id, text in documents:
        chunks = chunk_text(text)
        if not chunks:
            continue
        embeddings = embedding_provider.embed(chunks)
        total += store.upsert_chunks(corpus_id, doc_id, chunks, embeddings)
    return total
