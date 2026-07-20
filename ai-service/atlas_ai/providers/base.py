"""One adapter interface every LLM provider goes through — application code
(planner.py, explain.py) never branches on which provider is configured;
only `LiteLLMProvider` knows that `ATLAS_LLM_PROVIDER`/`ATLAS_LLM_MODEL`
become a litellm model string.
"""

from __future__ import annotations

from typing import Protocol


class ModelProvider(Protocol):
    def complete(self, prompt: str, **kwargs) -> str:
        """Returns the model's raw text completion for `prompt`."""
        ...
