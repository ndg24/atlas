"""LiteLLMEmbeddingProvider is a thin wrapper over litellm.embedding() --
mocked here the same way test_nl_to_plan.py avoids real LLM calls in CI, by
monkeypatching litellm.embedding rather than hitting a real provider.
"""

from __future__ import annotations

import atlas_ai.retrieval.embeddings as embeddings_module
from atlas_ai.retrieval.embeddings import LiteLLMEmbeddingProvider


def test_embed_builds_provider_model_string_and_returns_vectors_in_order(monkeypatch):
    captured = {}

    def fake_embedding(model, input):
        captured["model"] = model
        captured["input"] = input
        return {"data": [{"embedding": [1.0, 0.0]}, {"embedding": [0.0, 1.0]}]}

    monkeypatch.setattr(embeddings_module.litellm, "embedding", fake_embedding)

    provider = LiteLLMEmbeddingProvider("ollama", "nomic-embed-text")
    result = provider.embed(["first", "second"])

    assert captured["model"] == "ollama/nomic-embed-text"
    assert captured["input"] == ["first", "second"]
    assert result == [[1.0, 0.0], [0.0, 1.0]]
