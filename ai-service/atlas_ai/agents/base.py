"""One interface every pipeline stage implements — pipeline.py never branches
on which agent it's running, only calls `.run(state)` in fixed order.
"""

from __future__ import annotations

from typing import Protocol

from .state import PipelineState


class Agent(Protocol):
    def run(self, state: PipelineState) -> PipelineState: ...
