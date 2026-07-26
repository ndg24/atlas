"""run_pipeline: the sequential Planner -> Execution -> Visualization ->
Explanation -> Report pipeline (docs/atlas-implementation-spec.md Phase 8).
A plain fixed-order loop over a typed, accumulating PipelineState -- not a
graph-orchestration framework, since nothing here needs branching or
looping. Query and Execution are one combined agent (ExecutionAgent), per
task 3's own framing of them as thin wrappers around already-built code
rather than separate query logic.
"""

from __future__ import annotations

import logging

from ..providers.base import ModelProvider
from .coordinator_client import CoordinatorClient
from .execution_agent import ExecutionAgent
from .explanation_agent import ExplanationAgent
from .planner_agent import PlannerAgent
from .report_agent import ReportAgent
from .state import PipelineState
from .visualization_agent import VisualizationAgent

logger = logging.getLogger(__name__)


def run_pipeline(
    question: str,
    dataset: str,
    schema_json: str,
    corpus_id: str,
    coordinator_url: str,
    auth_token: str,
    provider: ModelProvider,
) -> PipelineState:
    state = PipelineState(question=question, dataset=dataset, schema_json=schema_json, corpus_id=corpus_id)
    query_runner = CoordinatorClient(coordinator_url, auth_token)

    agents = [
        PlannerAgent(provider),
        ExecutionAgent(query_runner),
        VisualizationAgent(),
        ExplanationAgent(provider),
        ReportAgent(),
    ]
    for agent in agents:
        before = state.model_dump()
        state = agent.run(state)
        logger.info("agent=%s in=%s out=%s", type(agent).__name__, before, state.model_dump())
    return state
