"""~15 (question, canned plan, expected shape) fixtures for the golden
structural test suite (docs/atlas-implementation-spec.md Phase 6, task 6).
Assertions check node types / columns referenced / aggregate functions —
never exact LLM text, since a real provider's wording is nondeterministic.

All cases share one schema: diagnosis/hospital (Utf8), age (Int64),
cost (Float64) — the same shape engine/crates/atlas-worker/src/service.rs's
tests and atlas-query's SQL tests already use, so an NL golden case and a
hand-written SQL equivalent stay directly comparable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json

SCHEMA_JSON = json.dumps(
    {
        "fields": [
            {"name": "diagnosis", "data_type": "Utf8"},
            {"name": "age", "data_type": "Int64"},
            {"name": "cost", "data_type": "Float64"},
            {"name": "hospital", "data_type": "Utf8"},
        ]
    }
)


def scan():
    return {"Scan": {"dataset": "t", "columns": [], "snapshot_id": ""}}


def col(name):
    return {"Column": name}


def lit_int(n):
    return {"Literal": {"Int": n}}


def lit_float(f):
    return {"Literal": {"Float": f}}


def lit_str(s):
    return {"Literal": {"Str": s}}


def binary(left, op, right):
    return {"Binary": {"left": left, "op": op, "right": right}}


def filter_(input_, predicate):
    return {"Filter": {"input": input_, "predicate": predicate}}


def project(input_, exprs, aliases):
    return {"Project": {"input": input_, "exprs": exprs, "aliases": aliases}}


def aggregate(input_, group_by, aggregates):
    return {"Aggregate": {"input": input_, "group_by": group_by, "aggregates": aggregates}}


def agg_expr(func, alias, arg=None):
    return {"func": func, "arg": arg, "alias": alias}


def sort(input_, keys):
    return {"Sort": {"input": input_, "keys": keys}}


def sort_key(expr, descending=False):
    return {"expr": expr, "descending": descending}


def limit(input_, n):
    return {"Limit": {"input": input_, "n": n}}


@dataclass
class GoldenCase:
    id: str
    question: str
    plan: dict
    expect_root: str
    expect_columns: set[str]
    expect_agg_funcs: set[str] = field(default_factory=set)


CASES: list[GoldenCase] = [
    GoldenCase(
        id="simple_project",
        question="List the diagnosis and age for every patient.",
        plan=project(scan(), [col("diagnosis"), col("age")], ["diagnosis", "age"]),
        expect_root="Project",
        expect_columns={"diagnosis", "age"},
    ),
    GoldenCase(
        id="filter_gt",
        question="Which diagnoses belong to patients older than 50?",
        plan=project(
            filter_(scan(), binary(col("age"), "Gt", lit_int(50))),
            [col("diagnosis")],
            ["diagnosis"],
        ),
        expect_root="Project",
        expect_columns={"diagnosis", "age"},
    ),
    GoldenCase(
        id="filter_lt",
        question="Which diagnoses cost less than $100?",
        plan=project(
            filter_(scan(), binary(col("cost"), "Lt", lit_float(100.0))),
            [col("diagnosis")],
            ["diagnosis"],
        ),
        expect_root="Project",
        expect_columns={"diagnosis", "cost"},
    ),
    GoldenCase(
        id="filter_eq_string",
        question="Show me every record for the diagnosis 'flu'.",
        plan=project(
            filter_(scan(), binary(col("diagnosis"), "Eq", lit_str("flu"))),
            [col("diagnosis"), col("hospital")],
            ["diagnosis", "hospital"],
        ),
        expect_root="Project",
        expect_columns={"diagnosis", "hospital"},
    ),
    GoldenCase(
        id="filter_neq",
        question="Which hospitals are not General Hospital?",
        plan=project(
            filter_(scan(), binary(col("hospital"), "NotEq", lit_str("General Hospital"))),
            [col("hospital")],
            ["hospital"],
        ),
        expect_root="Project",
        expect_columns={"hospital"},
    ),
    GoldenCase(
        id="count_group_by",
        question="How many patients have each diagnosis?",
        plan=aggregate(scan(), [col("diagnosis")], [agg_expr("Count", "n")]),
        expect_root="Aggregate",
        expect_columns={"diagnosis"},
        expect_agg_funcs={"Count"},
    ),
    GoldenCase(
        id="sum_group_by",
        question="What is the total cost per diagnosis?",
        plan=aggregate(scan(), [col("diagnosis")], [agg_expr("Sum", "total_cost", col("cost"))]),
        expect_root="Aggregate",
        expect_columns={"diagnosis", "cost"},
        expect_agg_funcs={"Sum"},
    ),
    GoldenCase(
        id="avg_group_by",
        question="What is the average age of patients per diagnosis?",
        plan=aggregate(scan(), [col("diagnosis")], [agg_expr("Avg", "avg_age", col("age"))]),
        expect_root="Aggregate",
        expect_columns={"diagnosis", "age"},
        expect_agg_funcs={"Avg"},
    ),
    GoldenCase(
        id="min_max_group_by",
        question="What are the minimum and maximum cost per hospital?",
        plan=aggregate(
            scan(),
            [col("hospital")],
            [agg_expr("Min", "min_cost", col("cost")), agg_expr("Max", "max_cost", col("cost"))],
        ),
        expect_root="Aggregate",
        expect_columns={"hospital", "cost"},
        expect_agg_funcs={"Min", "Max"},
    ),
    GoldenCase(
        id="top_n",
        question="What are the 5 most common diagnoses?",
        plan=limit(
            sort(
                aggregate(scan(), [col("diagnosis")], [agg_expr("Count", "n")]),
                [sort_key(col("n"), descending=True)],
            ),
            5,
        ),
        expect_root="Limit",
        expect_columns={"diagnosis", "n"},
        expect_agg_funcs={"Count"},
    ),
    GoldenCase(
        id="sort_ascending",
        question="List diagnoses sorted by cost, cheapest first.",
        plan=sort(
            project(scan(), [col("diagnosis"), col("cost")], ["diagnosis", "cost"]),
            [sort_key(col("cost"), descending=False)],
        ),
        expect_root="Sort",
        expect_columns={"diagnosis", "cost"},
    ),
    GoldenCase(
        id="and_predicate",
        question="Which diagnoses are for patients over 50 with cost under $1000?",
        plan=project(
            filter_(
                scan(),
                binary(
                    binary(col("age"), "Gt", lit_int(50)),
                    "And",
                    binary(col("cost"), "Lt", lit_float(1000.0)),
                ),
            ),
            [col("diagnosis")],
            ["diagnosis"],
        ),
        expect_root="Project",
        expect_columns={"diagnosis", "age", "cost"},
    ),
    GoldenCase(
        id="or_predicate",
        question="Which records are for General Hospital or City Hospital?",
        plan=project(
            filter_(
                scan(),
                binary(
                    binary(col("hospital"), "Eq", lit_str("General Hospital")),
                    "Or",
                    binary(col("hospital"), "Eq", lit_str("City Hospital")),
                ),
            ),
            [col("diagnosis"), col("hospital")],
            ["diagnosis", "hospital"],
        ),
        expect_root="Project",
        expect_columns={"diagnosis", "hospital"},
    ),
    GoldenCase(
        id="limit_only",
        question="Show me 10 patient records.",
        plan=limit(project(scan(), [col("diagnosis"), col("age")], ["diagnosis", "age"]), 10),
        expect_root="Limit",
        expect_columns={"diagnosis", "age"},
    ),
    GoldenCase(
        id="multi_group_by",
        question="How many patients per diagnosis and hospital?",
        plan=aggregate(scan(), [col("diagnosis"), col("hospital")], [agg_expr("Count", "n")]),
        expect_root="Aggregate",
        expect_columns={"diagnosis", "hospital"},
        expect_agg_funcs={"Count"},
    ),
]

assert len(CASES) >= 15, "Phase 6 DoD requires a ~15-question golden suite"
