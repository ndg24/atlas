"""Literature retrieval over an ingested corpus (docs/atlas-implementation-spec.md
Phase 8, task 4): embeddings-backed nearest-neighbor search over `pgvector`,
returning the documents most relevant to a literature sub-question.

Not implemented yet — `retrieve` always returns no documents. Corpus
ingestion (PDFs/abstracts), embeddings, and the `pgvector` lookup itself are
a fast-follow slice; the pipeline (`../agents/report_agent.py`) already calls
this function and tags whatever it returns `[literature:doc_id]`, so wiring
in the real implementation later needs no change to the pipeline shape. Its
being a stub is also why every claim the pipeline currently produces is
`[data]`-tagged: with no documents ever returned, no `[literature:...]` claim
can appear.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Document:
    doc_id: str
    text: str
    score: float


def retrieve(query: str, corpus_id: str, k: int = 5) -> list[Document]:
    return []
