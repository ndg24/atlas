"""End-to-end retrieval test against a real Postgres+pgvector instance --
gated the same way test_nl_to_plan.py gates real multi-provider LLM calls
(ATLAS_AI_INTEGRATION=1), since this needs real infra (a reachable Postgres
with the `vector` extension and the corpus_documents table from
coordinator/internal/catalog/migrations/0008_corpus_documents.up.sql) that
CI doesn't run by default. `deploy/docker-compose.yml`'s postgres service
(pgvector/pgvector:pg16) plus the coordinator's migration runner satisfies
both prerequisites.

Uses a fake EmbeddingProvider (deterministic, not a real model call) so this
test only exercises the pgvector wiring itself (chunking, storage, cosine
search), not an actual embedding model.
"""

from __future__ import annotations

import os

import pytest

from atlas_ai.retrieval.ingest import ingest_documents
from atlas_ai.retrieval.store import PgVectorStore

_INTEGRATION = os.environ.get("ATLAS_AI_INTEGRATION") == "1"
_DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://atlas:atlas@localhost:5432/atlas")


class FakeEmbeddingProvider:
    """Maps each text deterministically to a point in 768-dim space based on
    a keyword's position, so "risk factor" texts embed near each other and
    far from "unrelated" texts -- enough to prove nearest-neighbor ranking
    without a real model."""

    def embed(self, texts: list[str]) -> list[list[float]]:
        vectors = []
        for text in texts:
            vec = [0.0] * 768
            if "risk" in text.lower():
                vec[0] = 1.0
            else:
                vec[1] = 1.0
            vectors.append(vec)
        return vectors


def _corpus_available() -> bool:
    try:
        PgVectorStore(_DATABASE_URL).search("__probe__", [0.0] * 768, 1)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _INTEGRATION or not _corpus_available(),
    reason="set ATLAS_AI_INTEGRATION=1 with a reachable Postgres+pgvector (corpus_documents table present) to run",
)


def test_ingest_then_retrieve_ranks_the_relevant_document_first():
    corpus_id = "test-corpus-ingest-retrieve"
    provider = FakeEmbeddingProvider()

    ingest_documents(
        corpus_id,
        [
            ("doc-risk", "Known risk factors for readmission include age and prior admissions."),
            ("doc-unrelated", "The hospital cafeteria serves lunch from noon to two."),
        ],
        _DATABASE_URL,
        provider,
    )

    [query_embedding] = provider.embed(["what are the risk factors?"])
    results = PgVectorStore(_DATABASE_URL).search(corpus_id, query_embedding, k=5)

    assert results[0][0] == "doc-risk"
    assert results[0][2] > results[1][2]  # top result scores higher than the unrelated one
