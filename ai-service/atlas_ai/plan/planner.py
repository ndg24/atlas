"""`nl_to_plan`: question + schema -> validated LogicalPlan JSON.

docs/atlas-implementation-spec.md Phase 6, task 2: parse the LLM's JSON
output, validate it (schema.validate_logical_plan); on failure, re-prompt
once with the validation error appended, then give up and raise a clear
error after that second attempt.
"""

from __future__ import annotations

import json

from ..providers.base import ModelProvider
from .prompt import build_prompt, build_retry_prompt
from .schema import PlanValidationError, column_names, validate_logical_plan


class NLToPlanError(Exception):
    """Raised after both the first attempt and the one allowed retry fail —
    the caller (server.py's NLToQuery handler) turns this into
    NLResponse.error rather than a gRPC-level failure, mirroring how a bad
    SQL query surfaces as CompileResponse.error rather than an RPC error."""


def _strip_code_fence(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        if text.endswith("```"):
            text = text[: -len("```")]
    return text.strip()


def _parse_and_validate(raw_output: str, fields: set[str]) -> dict:
    try:
        plan = json.loads(_strip_code_fence(raw_output))
    except json.JSONDecodeError as exc:
        raise PlanValidationError(f"output is not valid JSON: {exc}") from exc
    validate_logical_plan(plan, fields)
    return plan


def nl_to_plan(question: str, schema_json: str, provider: ModelProvider) -> tuple[str, str]:
    """Returns (plan_json, raw_llm_output) for the *first* attempt whose
    output validated — raw_llm_output is always the last completion the
    model produced, win or lose, so the caller can inspect what happened."""
    fields = column_names(schema_json)

    prompt = build_prompt(question, schema_json)
    raw_output = provider.complete(prompt)
    try:
        plan = _parse_and_validate(raw_output, fields)
        return json.dumps(plan), raw_output
    except PlanValidationError as first_error:
        retry_prompt = build_retry_prompt(question, schema_json, raw_output, str(first_error))
        raw_output = provider.complete(retry_prompt)
        try:
            plan = _parse_and_validate(raw_output, fields)
            return json.dumps(plan), raw_output
        except PlanValidationError as second_error:
            raise NLToPlanError(
                f"question {question!r} failed to compile to a valid plan after 1 retry: {second_error}"
            ) from second_error
