"""PgVectorStore: corpus document chunks + embeddings, held in the existing
Postgres instance behind the `vector` extension
(coordinator/internal/catalog/migrations/0008_corpus_documents.up.sql) rather
than a second datastore -- Phase 8 task 4's explicit design choice. Exact
nearest-neighbor via `<=>` (cosine distance) ORDER BY + LIMIT, no
ivfflat/approximate index: at the fixture/small-corpus scale this slice
targets, a brute-force scan is simpler and exactly correct.
"""

from __future__ import annotations

import psycopg
from pgvector import Vector
from pgvector.psycopg import register_vector


class PgVectorStore:
    def __init__(self, database_url: str):
        self._database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self._database_url, autocommit=True)
        register_vector(conn)
        return conn

    def upsert_chunks(self, corpus_id: str, doc_id: str, chunks: list[str], embeddings: list[list[float]]) -> int:
        """Replaces every existing chunk for (corpus_id, doc_id) with the
        given chunks/embeddings -- re-ingesting a document is idempotent
        rather than accumulating duplicate rows across runs."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM corpus_documents WHERE corpus_id = %s AND doc_id = %s",
                    (corpus_id, doc_id),
                )
                for index, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                    cur.execute(
                        """INSERT INTO corpus_documents (corpus_id, doc_id, chunk_index, text, embedding)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (corpus_id, doc_id, index, chunk, Vector(embedding)),
                    )
        return len(chunks)

    def search(self, corpus_id: str, query_embedding: list[float], k: int) -> list[tuple[str, str, float]]:
        """Returns up to k (doc_id, text, score) tuples for the corpus,
        nearest first. score is cosine similarity (1 - cosine distance), so
        higher is more relevant."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT doc_id, text, 1 - (embedding <=> %s) AS score
                       FROM corpus_documents
                       WHERE corpus_id = %s
                       ORDER BY embedding <=> %s
                       LIMIT %s""",
                    (Vector(query_embedding), corpus_id, Vector(query_embedding), k),
                )
                return cur.fetchall()
