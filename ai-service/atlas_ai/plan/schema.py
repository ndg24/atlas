"""Hand-written mirror of `atlas_query::plan::LogicalPlan`
(engine/crates/atlas-query/src/plan.rs) — the third mirror of that shape
(Rust's own serde derive is the first, Go's coordinator/internal/planjson
is the second, narrower one), per proto/plan.proto's own header comment:
every consumer hand-writes its mirror rather than compiling that .proto.

`validate_logical_plan` checks that a dict the LLM produced already has
serde's *external* tagging exactly: `{"Scan": {...}}`, `{"Column": "x"}`,
`{"Binary": {"left": ..., "op": "Gt", "right": ...}}`, unit enum variants
(`AggFunc`, `BinaryOp`) as bare strings. Validation never re-shapes the
dict — the input *is* the plan_json payload once it passes, so there is no
separate re-serialization step that could accidentally drift from what
`compile_query_from_plan` (engine/crates/atlas-worker/src/service.rs)
expects to deserialize.

Column references are checked against the dataset's actual Arrow schema
(arrow-schema's own `Schema`/`Field` serde shape:
`{"fields": [{"name": ..., "data_type": ..., ...}], "metadata": {...}}`,
produced by `serde_json::to_string(&schema)` — see
engine/crates/atlas-cli/src/main.rs and atlas-format/src/writer.rs) so a
hallucinated column name fails validation before ever reaching the engine.
"""

from __future__ import annotations

import json

BINARY_OPS = {
    "Eq", "NotEq", "Lt", "LtEq", "Gt", "GtEq", "And", "Or", "Add", "Sub", "Mul", "Div",
}
AGG_FUNCS = {"Count", "Sum", "Avg", "Min", "Max"}
LITERAL_KINDS = {"Int", "Float", "Str", "Bool"}
PLAN_NODE_KINDS = {"Scan", "Filter", "Project", "Aggregate", "Sort", "Limit"}


class PlanValidationError(ValueError):
    """Raised with a `path` describing where in the plan validation failed —
    fed back to the LLM verbatim as the one re-prompt hint `nl_to_plan`
    allows (docs/atlas-implementation-spec.md Phase 6, task 2)."""


def column_names(schema_json: str) -> set[str]:
    try:
        schema = json.loads(schema_json)
    except json.JSONDecodeError as exc:
        raise PlanValidationError(f"schema_json is not valid JSON: {exc}") from exc
    fields = schema.get("fields") if isinstance(schema, dict) else None
    if not isinstance(fields, list):
        raise PlanValidationError('schema_json missing a "fields" array')
    names = set()
    for field in fields:
        if not isinstance(field, dict) or "name" not in field:
            raise PlanValidationError(f'schema_json field missing "name": {field!r}')
        names.add(field["name"])
    return names


def _single_key(node: object, path: str) -> tuple[str, object]:
    if not isinstance(node, dict) or len(node) != 1:
        raise PlanValidationError(
            f"{path}: expected a single-key object naming the node type, got {node!r}"
        )
    (kind, body), = node.items()
    return kind, body


def _validate_expr(expr: object, fields: set[str], path: str) -> None:
    kind, body = _single_key(expr, path)
    if kind == "Column":
        if not isinstance(body, str):
            raise PlanValidationError(f"{path}.Column: expected a string column name, got {body!r}")
        if body not in fields:
            raise PlanValidationError(
                f"{path}.Column: {body!r} is not a column in this dataset's schema "
                f"(available: {sorted(fields)})"
            )
    elif kind == "Literal":
        lit_kind, lit_val = _single_key(body, f"{path}.Literal")
        if lit_kind not in LITERAL_KINDS:
            raise PlanValidationError(
                f"{path}.Literal: unknown literal kind {lit_kind!r} (expected one of {sorted(LITERAL_KINDS)})"
            )
        if lit_kind == "Int" and not isinstance(lit_val, int):
            raise PlanValidationError(f"{path}.Literal.Int: expected an integer, got {lit_val!r}")
        if lit_kind == "Float" and not isinstance(lit_val, (int, float)):
            raise PlanValidationError(f"{path}.Literal.Float: expected a number, got {lit_val!r}")
        if lit_kind == "Str" and not isinstance(lit_val, str):
            raise PlanValidationError(f"{path}.Literal.Str: expected a string, got {lit_val!r}")
        if lit_kind == "Bool" and not isinstance(lit_val, bool):
            raise PlanValidationError(f"{path}.Literal.Bool: expected a boolean, got {lit_val!r}")
    elif kind == "Binary":
        if not isinstance(body, dict) or {"left", "op", "right"} - body.keys():
            raise PlanValidationError(
                f'{path}.Binary: expected an object with "left", "op", "right", got {body!r}'
            )
        if body["op"] not in BINARY_OPS:
            raise PlanValidationError(
                f"{path}.Binary.op: unknown operator {body['op']!r} (expected one of {sorted(BINARY_OPS)})"
            )
        _validate_expr(body["left"], fields, f"{path}.Binary.left")
        _validate_expr(body["right"], fields, f"{path}.Binary.right")
    else:
        raise PlanValidationError(
            f"{path}: unknown Expr kind {kind!r} (expected Column/Literal/Binary)"
        )


def _require_keys(body: object, keys: set[str], path: str) -> dict:
    if not isinstance(body, dict) or keys - body.keys():
        raise PlanValidationError(f"{path}: expected an object with keys {sorted(keys)}, got {body!r}")
    return body


def validate_logical_plan(node: object, fields: set[str], path: str = "plan") -> set[str]:
    """Raises PlanValidationError on the first structural problem found.
    Returns the set of column names available to whatever node sits on top
    of `node` — Scan/Filter/Sort/Limit pass columns through unchanged,
    Project narrows to its aliases, Aggregate narrows to its group-by
    columns plus each aggregate's alias. This matters because a Sort or
    Project stacked on top of an Aggregate legitimately references the
    Aggregate's *output* names (e.g. `ORDER BY n` after `COUNT(*) AS n`),
    which don't exist in the base dataset schema `fields` at all — checking
    every column reference against the original `fields` regardless of
    plan depth would reject every non-trivial aggregate query.
    """
    kind, body = _single_key(node, path)
    if kind not in PLAN_NODE_KINDS:
        raise PlanValidationError(
            f"{path}: unknown LogicalPlan node {kind!r} (expected one of {sorted(PLAN_NODE_KINDS)})"
        )
    node_path = f"{path}.{kind}"

    if kind == "Scan":
        body = _require_keys(body, {"dataset", "columns", "snapshot_id"}, node_path)
        if not isinstance(body["columns"], list) or not all(isinstance(c, str) for c in body["columns"]):
            raise PlanValidationError(f"{node_path}.columns: expected a list of strings")
        for col in body["columns"]:
            if col not in fields:
                raise PlanValidationError(
                    f"{node_path}.columns: {col!r} is not a column in this dataset's schema "
                    f"(available: {sorted(fields)})"
                )
        return set(body["columns"]) if body["columns"] else set(fields)

    if kind == "Filter":
        body = _require_keys(body, {"input", "predicate"}, node_path)
        input_cols = validate_logical_plan(body["input"], fields, f"{node_path}.input")
        _validate_expr(body["predicate"], input_cols, f"{node_path}.predicate")
        return input_cols

    if kind == "Project":
        body = _require_keys(body, {"input", "exprs", "aliases"}, node_path)
        input_cols = validate_logical_plan(body["input"], fields, f"{node_path}.input")
        if not isinstance(body["exprs"], list):
            raise PlanValidationError(f"{node_path}.exprs: expected a list")
        for i, expr in enumerate(body["exprs"]):
            _validate_expr(expr, input_cols, f"{node_path}.exprs[{i}]")
        if not isinstance(body["aliases"], list) or len(body["aliases"]) != len(body["exprs"]):
            raise PlanValidationError(f"{node_path}.aliases: must be a list the same length as exprs")
        return set(body["aliases"])

    if kind == "Aggregate":
        body = _require_keys(body, {"input", "group_by", "aggregates"}, node_path)
        input_cols = validate_logical_plan(body["input"], fields, f"{node_path}.input")
        if not isinstance(body["group_by"], list):
            raise PlanValidationError(f"{node_path}.group_by: expected a list")
        for i, expr in enumerate(body["group_by"]):
            _validate_expr(expr, input_cols, f"{node_path}.group_by[{i}]")
        if not isinstance(body["aggregates"], list) or not body["aggregates"]:
            raise PlanValidationError(f"{node_path}.aggregates: expected a non-empty list")
        output_cols = {expr["Column"] for expr in body["group_by"] if isinstance(expr, dict) and "Column" in expr}
        for i, agg in enumerate(body["aggregates"]):
            agg_path = f"{node_path}.aggregates[{i}]"
            agg = _require_keys(agg, {"func", "arg", "alias"}, agg_path)
            if agg["func"] not in AGG_FUNCS:
                raise PlanValidationError(
                    f"{agg_path}.func: unknown aggregate {agg['func']!r} (expected one of {sorted(AGG_FUNCS)})"
                )
            if agg["arg"] is not None:
                _validate_expr(agg["arg"], input_cols, f"{agg_path}.arg")
            elif agg["func"] != "Count":
                raise PlanValidationError(f"{agg_path}: only Count may omit arg (COUNT(*))")
            if not isinstance(agg["alias"], str) or not agg["alias"]:
                raise PlanValidationError(f"{agg_path}.alias: expected a non-empty string")
            output_cols.add(agg["alias"])
        return output_cols

    if kind == "Sort":
        body = _require_keys(body, {"input", "keys"}, node_path)
        input_cols = validate_logical_plan(body["input"], fields, f"{node_path}.input")
        if not isinstance(body["keys"], list) or not body["keys"]:
            raise PlanValidationError(f"{node_path}.keys: expected a non-empty list")
        for i, key in enumerate(body["keys"]):
            key_path = f"{node_path}.keys[{i}]"
            key = _require_keys(key, {"expr", "descending"}, key_path)
            _validate_expr(key["expr"], input_cols, f"{key_path}.expr")
            if not isinstance(key["descending"], bool):
                raise PlanValidationError(f"{key_path}.descending: expected a boolean")
        return input_cols

    if kind == "Limit":
        body = _require_keys(body, {"input", "n"}, node_path)
        input_cols = validate_logical_plan(body["input"], fields, f"{node_path}.input")
        if not isinstance(body["n"], int) or isinstance(body["n"], bool) or body["n"] < 0:
            raise PlanValidationError(f"{node_path}.n: expected a non-negative integer")
        return input_cols
