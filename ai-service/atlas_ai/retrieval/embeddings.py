"""EmbeddingProvider: the retrieval-side counterpart to providers/base.py's
ModelProvider -- one adapter interface so corpus ingestion and query-time
embedding both go through litellm.embedding() regardless of
ATLAS_EMBEDDING_PROVIDER/ATLAS_EMBEDDING_MODEL, mirroring the completion
path's provider-agnostic design (providers/litellm_provider.py) rather than
introducing a second, differently-shaped BYO-model story for embeddings.
"""

from __future__ import annotations

from typing import Protocol

import litellm


class EmbeddingProvider(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Returns one embedding vector per input text, same order."""
        ...


class LiteLLMEmbeddingProvider(EmbeddingProvider):
    def __init__(self, provider: str, model: str):
        self._model = f"{provider}/{model}"

    def embed(self, texts: list[str]) -> list[list[float]]:
        response = litellm.embedding(model=self._model, input=texts)
        return [item["embedding"] for item in response["data"]]
