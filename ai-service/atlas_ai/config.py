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
        )
