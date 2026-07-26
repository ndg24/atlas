"""ExecutionAgent: combines Phase 8's QueryAgent + ExecutionAgent (task 3) --
for each structured sub-question, calls the coordinator's own POST /query/nl
(the same NL-compile-and-execute path every other query goes through) and
decodes the returned Arrow IPC batches into rows, the same stream-decode
explain.py's narrate_result already does over an executed result. A
sub-question the coordinator can't answer is skipped, not fatal -- the same
best-effort convention the coordinator's own outlier/trend heuristics use
(coordinator/internal/api/insights.go's runOutlierAnalyze/runTrendAnalyze).
Literature sub-questions are left for ReportAgent.
"""

from __future__ import annotations

import base64
import io
import logging

import pyarrow.ipc as ipc

from .coordinator_client import QueryRunner
from .state import PipelineState, QueryResult

logger = logging.getLogger(__name__)


def _decode_rows(arrow_ipc_batches_b64: list[str]) -> list[dict]:
    rows: list[dict] = []
    for b64 in arrow_ipc_batches_b64:
        raw = base64.b64decode(b64) if b64 else b""
        if not raw:
            continue
        with ipc.open_stream(io.BytesIO(raw)) as reader:
            rows.extend(reader.read_all().to_pylist())
    return rows


class ExecutionAgent:
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def run(self, state: PipelineState) -> PipelineState:
        for sub_question in state.sub_questions:
            if sub_question.kind != "structured":
                continue
            try:
                resp = self._query_runner.query_nl(state.dataset, sub_question.text)
            except Exception as exc:
                logger.info("skipping sub-question %r: /query/nl failed (%s)", sub_question.text, exc)
                continue
            rows = _decode_rows(resp.get("arrow_ipc_batches", []))
            state.results.append(QueryResult(sub_question=sub_question.text, rows=rows, row_count=len(rows)))
        return state
