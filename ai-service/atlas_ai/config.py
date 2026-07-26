"""Env-var config loading, matching the Go coordinator's `envOr` convention
(coordinator/cmd/coordinator/main.go) — plain os.environ reads with an
explicit default, no config file or settings library.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def env_or(key: str, default: str) -> str:
    return os.environ.get(key) or default


@dataclass(frozen=True)
class Config:
    llm_provider: str
    llm_model: str
    listen_addr: str
    coordinator_url: str
    # Phase 8 literature retrieval (atlas_ai/retrieval): embeddings go
    # through litellm the same way completions do (embeddings.py mirrors
    # providers/litellm_provider.py), defaulting to the same ollama runtime
    # so BYO-model stays consistent across completion and embedding calls.
    embedding_provider: str
    embedding_model: str
    # corpus_documents (coordinator/internal/catalog/migrations/0008_*)
    # lives on the same Postgres instance as the catalog, not a second
    # datastore — the ai-service connects to it directly for retrieval,
    # bypassing CatalogService since corpus documents aren't dataset
    # metadata.
    database_url: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            # ollama is the first-class local default (README: "No hosted-LLM
            # API key required if ATLAS_LLM_PROVIDER=ollama") — never a
            # fallback bolted on after the fact.
            llm_provider=env_or("ATLAS_LLM_PROVIDER", "ollama"),
            llm_model=env_or("ATLAS_LLM_MODEL", "llama3"),
            listen_addr=env_or("AI_SERVICE_ADDR", "0.0.0.0:9092"),
            # Phase 8's ExecutionAgent calls back into the coordinator's own
            # POST /query/nl for each structured sub-question — this is where
            # it finds it. docker-compose points it at "http://coordinator:8080".
            coordinator_url=env_or("COORDINATOR_URL", "http://localhost:8080"),
            embedding_provider=env_or("ATLAS_EMBEDDING_PROVIDER", "ollama"),
            embedding_model=env_or("ATLAS_EMBEDDING_MODEL", "nomic-embed-text"),
            database_url=env_or("DATABASE_URL", "postgres://atlas:atlas@localhost:5432/atlas"),
        )
