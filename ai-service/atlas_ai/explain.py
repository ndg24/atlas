"""`narrate_result`: turn an already-executed result set into plain English.

The LLM only ever sees this result table (Arrow IPC bytes the engine already
produced, deserialized via pyarrow) — never raw source data — preserving the
"engine is source of truth" boundary (docs/atlas-implementation-spec.md
Phase 6, task 4). `result_arrow_ipc` is a complete IPC *stream* (schema +
batches), matching engine/crates/atlas-worker/src/ipc.rs's `encode_batches`.
"""

from __future__ import annotations

import io
import json

import pyarrow as pa
import pyarrow.ipc as ipc

from .providers.base import ModelProvider

_PROMPT_TEMPLATE = """You are given the result of a data query as a table, and the
question it answers. Write a short, plain-English answer.

Rules:
- State ONLY numbers that appear in the table below — never estimate,
  round to a value not present, or invent a figure.
- If the table is empty, say so plainly instead of guessing.

Question: "{question}"

Result table ({num_rows} rows):
{table_repr}

Answer:"""


def _read_table(result_arrow_ipc: bytes) -> pa.Table:
    if not result_arrow_ipc:
        return pa.table({})
    with ipc.open_stream(io.BytesIO(result_arrow_ipc)) as reader:
        return reader.read_all()


def narrate_result(question: str, result_arrow_ipc: bytes, provider: ModelProvider) -> str:
    table = _read_table(result_arrow_ipc)
    prompt = _PROMPT_TEMPLATE.format(
        question=question,
        num_rows=table.num_rows,
        table_repr=json.dumps(table.to_pylist()) if table.num_rows else "(empty)",
    )
    return provider.complete(prompt)
