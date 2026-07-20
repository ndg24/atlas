"""The one place ATLAS_LLM_PROVIDER/ATLAS_LLM_MODEL become a concrete
completion call — litellm's `<provider>/<model>` string form covers
Anthropic/OpenAI/Gemini/Ollama uniformly, so switching providers never needs
a branch here, only a different env var.
"""

from __future__ import annotations

import litellm

from .base import ModelProvider


class LiteLLMProvider(ModelProvider):
    def __init__(self, provider: str, model: str):
        self._model = f"{provider}/{model}"

    def complete(self, prompt: str, **kwargs) -> str:
        response = litellm.completion(
            model=self._model,
            messages=[{"role": "user", "content": prompt}],
            **kwargs,
        )
        return response["choices"][0]["message"]["content"]
