"""Prompt template for `nl_to_plan`: the dataset schema, a fixed set of
few-shot question -> plan-JSON examples (built against the same
diagnosis/age/cost schema atlas-query's own SQL tests use —
engine/crates/atlas-query/src/lib.rs — so NL and SQL golden cases stay
comparable), and an instruction to output nothing but the plan JSON.

Scan.columns is always left empty in these examples: column pruning is the
optimizer's job (atlas_optimizer::ColumnPruningRule, applied identically to
plans that started as SQL or as NL) — the AI service, like
atlas_query::build_logical_plan, only ever produces the raw (unpruned) plan.
"""

from __future__ import annotations

import json

_FEW_SHOT_SCHEMA = json.dumps(
    {
        "fields": [
            {"name": "diagnosis", "data_type": "Utf8"},
            {"name": "age", "data_type": "Int64"},
            {"name": "cost", "data_type": "Float64"},
        ]
    }
)

_EXAMPLES = [
    (
        "Which diagnoses and ages belong to patients older than 50?",
        {
            "Project": {
                "input": {
                    "Filter": {
                        "input": {"Scan": {"dataset": "t", "columns": [], "snapshot_id": ""}},
                        "predicate": {
                            "Binary": {
                                "left": {"Column": "age"},
                                "op": "Gt",
                                "right": {"Literal": {"Int": 50}},
                            }
                        },
                    }
                },
                "exprs": [{"Column": "diagnosis"}, {"Column": "age"}],
                "aliases": ["diagnosis", "age"],
            }
        },
    ),
    (
        "What are the 5 most common diagnoses among patients older than 50?",
        {
            "Limit": {
                "input": {
                    "Sort": {
                        "input": {
                            "Aggregate": {
                                "input": {
                                    "Filter": {
                                        "input": {"Scan": {"dataset": "t", "columns": [], "snapshot_id": ""}},
                                        "predicate": {
                                            "Binary": {
                                                "left": {"Column": "age"},
                                                "op": "Gt",
                                                "right": {"Literal": {"Int": 50}},
                                            }
                                        },
                                    }
                                },
                                "group_by": [{"Column": "diagnosis"}],
                                "aggregates": [{"func": "Count", "arg": None, "alias": "n"}],
                            }
                        },
                        "keys": [{"expr": {"Column": "n"}, "descending": True}],
                    }
                },
                "n": 5,
            }
        },
    ),
    (
        "Which diagnoses cost between $100 and $1000 for patients over 50?",
        {
            "Project": {
                "input": {
                    "Filter": {
                        "input": {"Scan": {"dataset": "t", "columns": [], "snapshot_id": ""}},
                        "predicate": {
                            "Binary": {
                                "left": {
                                    "Binary": {
                                        "left": {"Column": "age"},
                                        "op": "Gt",
                                        "right": {"Literal": {"Int": 50}},
                                    }
                                },
                                "op": "And",
                                "right": {
                                    "Binary": {
                                        "left": {"Column": "cost"},
                                        "op": "Gt",
                                        "right": {"Literal": {"Float": 100.0}},
                                    }
                                },
                            }
                        },
                    }
                },
                "exprs": [{"Column": "diagnosis"}],
                "aliases": ["diagnosis"],
            }
        },
    ),
]

_INSTRUCTIONS = """You translate a natural-language question about a dataset into a LogicalPlan.

Output ONLY a single JSON object matching the LogicalPlan shape below — no
prose, no markdown code fences, no explanation.

LogicalPlan is one of these node shapes, each a single-key object naming the
node type:
- {"Scan": {"dataset": str, "columns": [], "snapshot_id": ""}} — always leave
  columns empty; column selection is handled elsewhere.
- {"Filter": {"input": LogicalPlan, "predicate": Expr}}
- {"Project": {"input": LogicalPlan, "exprs": [Expr], "aliases": [str]}}
- {"Aggregate": {"input": LogicalPlan, "group_by": [Expr], "aggregates": [AggExpr]}}
- {"Sort": {"input": LogicalPlan, "keys": [{"expr": Expr, "descending": bool}]}}
- {"Limit": {"input": LogicalPlan, "n": int}}

Expr is one of:
- {"Column": "column_name"}
- {"Literal": {"Int": int}} | {"Literal": {"Float": float}} | {"Literal": {"Str": str}} | {"Literal": {"Bool": bool}}
- {"Binary": {"left": Expr, "op": OP, "right": Expr}} where OP is one of
  Eq, NotEq, Lt, LtEq, Gt, GtEq, And, Or, Add, Sub, Mul, Div

AggExpr is {"func": FUNC, "arg": Expr | null, "alias": str} where FUNC is one
of Count, Sum, Avg, Min, Max — arg is null only for COUNT(*).
"""


def build_prompt(question: str, schema_json: str) -> str:
    examples = "\n\n".join(
        f'Question: "{q}"\nPlan: {json.dumps(plan)}' for q, plan in _EXAMPLES
    )
    return (
        f"{_INSTRUCTIONS}\n"
        f"Example dataset schema:\n{_FEW_SHOT_SCHEMA}\n\n"
        f"{examples}\n\n"
        f"Now translate this question, using this dataset's actual schema.\n"
        f"Schema: {schema_json}\n"
        f'Question: "{question}"\n'
        f"Plan:"
    )


def build_retry_prompt(question: str, schema_json: str, bad_output: str, error: str) -> str:
    base = build_prompt(question, schema_json)
    return (
        f"{base}\n\n"
        f"Your previous answer was:\n{bad_output}\n\n"
        f"That failed validation: {error}\n"
        f"Fix it and resend ONLY the corrected JSON plan, nothing else."
    )
