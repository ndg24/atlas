"""`suggest_questions`: propose example questions a dataset's schema/summary
could answer, keeping only the ones that actually compile to a valid
LogicalPlan (docs/atlas-implementation-spec.md Phase 7, task 3) by round-
tripping each candidate through `nl_to_plan` -- so a suggested question is
guaranteed answerable, not just plausible-sounding.
"""

from __future__ import annotations

import json
import logging

from ..plan.planner import NLToPlanError, nl_to_plan
from ..plan.planner import _strip_code_fence
from ..providers.base import ModelProvider

logger = logging.getLogger(__name__)

_PROMPT_TEMPLATE = """You are given a dataset's schema and a statistical summary, as
JSON. Propose {count} example questions this dataset could answer.

Rules:
- Output ONLY a JSON array of question strings -- no prose, no markdown code fences.
- Every question must be answerable using only the columns in the schema below.
- Prefer questions that would produce a GROUP BY, a filter, or a sort -- not just
  "show me everything".

Schema:
{schema_json}

Summary:
{summary_json}

Questions:"""


def _parse_candidates(raw_output: str) -> list[str]:
    try:
        candidates = json.loads(_strip_code_fence(raw_output))
    except json.JSONDecodeError:
        return []
    if not isinstance(candidates, list):
        return []
    return [c for c in candidates if isinstance(c, str) and c.strip()]


def suggest_questions(
    schema_json: str, summary_json: str, provider: ModelProvider, count: int = 5
) -> list[str]:
    prompt = _PROMPT_TEMPLATE.format(count=count, schema_json=schema_json, summary_json=summary_json)
    raw_output = provider.complete(prompt)
    candidates = _parse_candidates(raw_output)

    valid: list[str] = []
    for question in candidates:
        try:
            nl_to_plan(question, schema_json, provider)
        except NLToPlanError as exc:
            logger.info("discarding suggested question %r: did not compile to a valid plan (%s)", question, exc)
            continue
        valid.append(question)
        if len(valid) >= count:
            break
    return valid
