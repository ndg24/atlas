"""VisualizationAgent: rule-based chart-type selection from a QueryResult's
row shape (docs/atlas-implementation-spec.md Phase 8, task 5) -- query shape
drives chart type, no LLM call. /query/nl's response doesn't carry the
LogicalPlan back, so the rule operates on the decoded rows/columns
themselves rather than plan structure. LLM-assisted disambiguation for
ambiguous shapes (the spec's stated fallback) isn't implemented in this
slice -- everything ambiguous just falls through to "table".
"""

from __future__ import annotations

from .state import ChartSpec, PipelineState, QueryResult


def _is_numeric(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _infer_chart(result: QueryResult) -> ChartSpec:
    if not result.rows:
        return ChartSpec(sub_question=result.sub_question, chart_type="table", reason="no rows returned")

    columns = list(result.rows[0].keys())
    numeric_cols = [c for c in columns if _is_numeric(result.rows[0][c])]
    non_numeric_cols = [c for c in columns if c not in numeric_cols]

    if len(result.rows) == 1 and len(columns) <= 2:
        return ChartSpec(
            sub_question=result.sub_question,
            chart_type="stat",
            y=numeric_cols[0] if numeric_cols else (columns[-1] if columns else None),
            reason="single-row result",
        )

    date_like = [c for c in non_numeric_cols if "date" in c.lower() or "time" in c.lower()]
    if date_like and numeric_cols:
        return ChartSpec(
            sub_question=result.sub_question,
            chart_type="line",
            x=date_like[0],
            y=numeric_cols[0],
            reason="a date-like column plus a numeric column suggests a trend over time",
        )

    if non_numeric_cols and numeric_cols:
        return ChartSpec(
            sub_question=result.sub_question,
            chart_type="bar",
            x=non_numeric_cols[0],
            y=numeric_cols[0],
            reason="a categorical column plus a numeric column suggests a grouped comparison",
        )

    return ChartSpec(sub_question=result.sub_question, chart_type="table", reason="no clear category/value pair to chart")


class VisualizationAgent:
    def run(self, state: PipelineState) -> PipelineState:
        for result in state.results:
            state.chart_specs.append(_infer_chart(result))
        return state
