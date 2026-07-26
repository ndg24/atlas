"""Literature retrieval over an ingested corpus (docs/atlas-implementation-spec.md
Phase 8, task 4): embeddings-backed nearest-neighbor search over `pgvector`
(store.py), returning the documents most relevant to a literature
sub-question. The pipeline (`../agents/report_agent.py`) calls `retrieve`
and tags whatever it returns `[literature:doc_id]` -- a corpus with no
ingested documents (or a request with no corpus_id) simply returns no
documents, so every claim in that report stays `[data]`-tagged, same
observable behavior as before this was wired in.

A failure here (Postgres unreachable, `vector` extension missing, embedding
call failing) is treated as best-effort and swallowed, same as
ExecutionAgent's "skip on failure, don't fail the whole pipeline" convention
for structured sub-questions -- literature retrieval is a research-report
enrichment, not something the whole pipeline should fail over.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from ..config import Config
from .embeddings import LiteLLMEmbeddingProvider
from .store import PgVectorStore

logger = logging.getLogger(__name__)


@dataclass
class Document:
    doc_id: str
    text: str
    score: float


def retrieve(query: str, corpus_id: str, k: int = 5) -> list[Document]:
    if not corpus_id:
        return []
    try:
        config = Config.from_env()
        provider = LiteLLMEmbeddingProvider(config.embedding_provider, config.embedding_model)
        [query_embedding] = provider.embed([query])
        rows = PgVectorStore(config.database_url).search(corpus_id, query_embedding, k)
    except Exception:
        logger.exception("retrieval failed for corpus %r; returning no documents", corpus_id)
        return []
    return [Document(doc_id=doc_id, text=text, score=score) for doc_id, text, score in rows]
