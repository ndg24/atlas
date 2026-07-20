"""Golden-file structural tests for nl_to_plan (docs/atlas-implementation-spec.md
Phase 6, task 6). Default run uses MockProvider — no network, no API keys,
safe for every PR. A second, real-provider pass is gated behind
ATLAS_AI_INTEGRATION=1 and runs the identical cases against >=2 providers,
proving the abstraction isn't accidentally provider-specific (Phase 6 DoD).
"""

from __future__ import annotations

import json
import os

import pytest

from atlas_ai.plan.planner import NLToPlanError, nl_to_plan
from atlas_ai.providers.litellm_provider import LiteLLMProvider

from .conftest import AlwaysBadProvider, FlakyThenGoodProvider, MockProvider
from .golden_cases import CASES, SCHEMA_JSON


def _collect(node, key):
    """Recursively collects every value found under `key` anywhere in a
    validated plan dict — e.g. every {"Column": "x"} or {"func": "Count"}."""
    found = set()

    def walk(n):
        if isinstance(n, dict):
            if key in n and isinstance(n[key], str):
                found.add(n[key])
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)

    walk(node)
    return found


@pytest.mark.parametrize("case", CASES, ids=[c.id for c in CASES])
def test_golden_case_structurally_correct(case):
    provider = MockProvider(case.plan)
    plan_json, raw_output = nl_to_plan(case.question, SCHEMA_JSON, provider)
    plan = json.loads(plan_json)
    assert raw_output, "expected a non-empty raw_llm_output"
    assert next(iter(plan)) == case.expect_root, f"expected root {case.expect_root!r}, got {next(iter(plan))!r}"
    assert case.expect_columns <= _collect(plan, "Column"), "not every expected column was referenced"
    if case.expect_agg_funcs:
        assert case.expect_agg_funcs <= _collect(plan, "func"), "not every expected aggregate function was used"
    assert len(provider.calls) == 1, "a valid first attempt should never trigger the retry prompt"


def test_retry_recovers_from_one_bad_attempt():
    good_plan = CASES[0].plan
    provider = FlakyThenGoodProvider(bad_response="not valid json{{{", good_response=good_plan)
    plan_json, raw_output = nl_to_plan(CASES[0].question, SCHEMA_JSON, provider)
    assert len(provider.calls) == 2, "expected exactly one retry after the first bad attempt"
    assert json.loads(plan_json) == good_plan
    assert json.loads(raw_output) == good_plan, "raw_llm_output should be the successful (2nd) completion"


def test_gives_up_after_one_retry():
    provider = AlwaysBadProvider()
    with pytest.raises(NLToPlanError):
        nl_to_plan("How many patients per diagnosis?", SCHEMA_JSON, provider)
    assert len(provider.calls) == 2, "expected exactly 2 attempts total (1 initial + 1 retry), then give up"


def test_hallucinated_column_fails_validation():
    bad_plan = {"Scan": {"dataset": "t", "columns": ["not_a_real_column"], "snapshot_id": ""}}
    provider = AlwaysBadProvider(bad_response=json.dumps(bad_plan))
    with pytest.raises(NLToPlanError, match="not_a_real_column"):
        nl_to_plan("Show me the not_a_real_column field.", SCHEMA_JSON, provider)


# --- Real-provider integration pass (Phase 6 DoD: >=2 providers, env-var-only switch) ---

_INTEGRATION = os.environ.get("ATLAS_AI_INTEGRATION") == "1"


@pytest.mark.skipif(not _INTEGRATION, reason="set ATLAS_AI_INTEGRATION=1 to run real-LLM golden tests")
@pytest.mark.parametrize("case", CASES, ids=[c.id for c in CASES])
def test_golden_case_against_ollama(case):
    provider = LiteLLMProvider("ollama", os.environ.get("ATLAS_LLM_MODEL", "llama3"))
    plan_json, _ = nl_to_plan(case.question, SCHEMA_JSON, provider)
    plan = json.loads(plan_json)
    assert next(iter(plan)) == case.expect_root


@pytest.mark.skipif(not _INTEGRATION, reason="set ATLAS_AI_INTEGRATION=1 to run real-LLM golden tests")
@pytest.mark.skipif(not os.environ.get("ANTHROPIC_API_KEY"), reason="no ANTHROPIC_API_KEY in the environment")
@pytest.mark.parametrize("case", CASES, ids=[c.id for c in CASES])
def test_golden_case_against_anthropic(case):
    provider = LiteLLMProvider("anthropic", os.environ.get("ATLAS_LLM_MODEL_ANTHROPIC", "claude-3-5-haiku-20241022"))
    plan_json, _ = nl_to_plan(case.question, SCHEMA_JSON, provider)
    plan = json.loads(plan_json)
    assert next(iter(plan)) == case.expect_root
