"""QueryRunner: how ExecutionAgent gets a structured sub-question answered —
by calling back into the coordinator's own POST /query/nl, the exact
NL-compile-and-execute path every other query already goes through, rather
than reimplementing scheduler/catalog logic here. Mirrors the ModelProvider
Protocol-plus-real-adapter pattern (../providers/base.py) so tests can
substitute a fake QueryRunner without mocking HTTP.
"""

from __future__ import annotations

from typing import Protocol

import httpx


class QueryRunner(Protocol):
    def query_nl(self, dataset: str, question: str) -> dict:
        """Returns the parsed JSON body of a POST /query/nl response.
        Raises on a non-2xx response — the caller (ExecutionAgent) treats
        that as best-effort and skips the sub-question rather than failing
        the whole pipeline."""
        ...


class CoordinatorClient:
    def __init__(self, base_url: str, auth_token: str):
        self._base_url = base_url.rstrip("/")
        self._auth_token = auth_token

    def query_nl(self, dataset: str, question: str) -> dict:
        resp = httpx.post(
            f"{self._base_url}/query/nl",
            json={"dataset": dataset, "question": question},
            headers={"Authorization": f"Bearer {self._auth_token}"},
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.json()
