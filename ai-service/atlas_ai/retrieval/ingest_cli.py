"""Small CLI for real-world corpus ingestion: reads every .txt file in a
directory, one document per file (doc_id = filename stem), and stores
chunked embeddings via ingest_documents.

Usage:
    python -m atlas_ai.retrieval.ingest_cli --corpus-id papers --dir ./papers

PDF extraction isn't wired in this slice -- pre-extract to .txt first (e.g.
via `pdftotext`) if your corpus is PDFs.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ..config import Config
from .embeddings import LiteLLMEmbeddingProvider
from .ingest import ingest_documents

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus-id", required=True)
    parser.add_argument("--dir", required=True, type=Path)
    args = parser.parse_args()

    documents = [(p.stem, p.read_text(encoding="utf-8")) for p in sorted(args.dir.glob("*.txt"))]
    if not documents:
        logger.warning("no .txt files found in %s", args.dir)
        return

    config = Config.from_env()
    provider = LiteLLMEmbeddingProvider(config.embedding_provider, config.embedding_model)
    total_chunks = ingest_documents(args.corpus_id, documents, config.database_url, provider)
    logger.info("ingested %d documents (%d chunks) into corpus %r", len(documents), total_chunks, args.corpus_id)


if __name__ == "__main__":
    main()
