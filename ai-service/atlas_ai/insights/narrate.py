"""`narrate_findings`: turn atlas-insights' already-computed structured
findings (a dataset's DatasetSummary plus QualityFinding / OutlierFinding /
TrendFinding lists, produced via WorkerService.Analyze -- see
engine/crates/atlas-insights) into plain English.

Same "engine is source of truth" boundary as explain.narrate_result: the LLM
only ever receives the finding objects themselves (docs/atlas-implementation-spec.md
Phase 7, task 3) -- one sentence per finding, no number that isn't already in
the input JSON.
"""

from __future__ import annotations

import json

from ..providers.base import ModelProvider

_PROMPT_TEMPLATE = """You are given a dataset's statistically-computed summary and
findings, as JSON. Write a short, plain-English narrative describing the dataset.

Rules:
- State ONLY numbers that appear in the JSON below -- never estimate, round to a
  value not present, or invent a figure.
- Write one sentence per finding (plus a short opening sentence about the summary,
  if a summary is present).
- If there are no findings at all, say the dataset looks unremarkable -- do not
  invent an issue to report.

Findings JSON:
{findings_json}

Narrative:"""


def narrate_findings(findings_json: str, provider: ModelProvider) -> str:
    # Round-trip through json.loads/dumps so malformed input fails loudly
    # here rather than silently reaching the prompt, and so formatting is
    # consistent regardless of how the coordinator serialized it.
    parsed = json.loads(findings_json) if findings_json else {}
    prompt = _PROMPT_TEMPLATE.format(findings_json=json.dumps(parsed))
    return provider.complete(prompt)
