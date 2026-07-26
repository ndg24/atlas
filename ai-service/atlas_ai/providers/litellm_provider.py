"""The one place ATLAS_LLM_PROVIDER/ATLAS_LLM_MODEL become a concrete
completion call — litellm's `<provider>/<model>` string form covers
Anthropic/OpenAI/Gemini/Ollama uniformly, so switching providers never needs
a branch here, only a different env var.
"""

from __future__ import annotations

import litellm

from ..telemetry import observe_llm_call, record_tokens
from .base import ModelProvider


class LiteLLMProvider(ModelProvider):
    def __init__(self, provider: str, model: str):
        self._provider = provider
        self._model_name = model
        self._model = f"{provider}/{model}"

    def complete(self, prompt: str, **kwargs) -> str:
        with observe_llm_call(self._provider, self._model_name):
            response = litellm.completion(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                **kwargs,
            )
        # Not every provider/model returns usage (e.g. some Ollama models) --
        # record zeros rather than skipping the call-count/latency metrics
        # observe_llm_call already recorded above.
        usage = getattr(response, "usage", None)
        record_tokens(
            self._provider,
            self._model_name,
            getattr(usage, "prompt_tokens", 0) or 0 if usage else 0,
            getattr(usage, "completion_tokens", 0) or 0 if usage else 0,
        )
        return response["choices"][0]["message"]["content"]
