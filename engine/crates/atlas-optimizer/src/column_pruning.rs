//! Rewrites the leaf `ScanNode.columns` to exactly the set of raw source
//! columns referenced anywhere in the plan.
//!
//! Only `Filter.predicate` and the single `Aggregate`/`Project` node's own
//! exprs reference genuine source columns — per `atlas_query`'s SQL builder,
//! the plan shape is always `Limit? > Sort? > (Aggregate | Project) >
//! Filter? > Scan`, so `Filter` always sits directly under `Scan` and
//! `Aggregate`/`Project` always sit directly over it. `Sort`'s keys and
//! `Limit` reference the *output* namespace `Aggregate`/`Project` produces
//! (e.g. `ORDER BY n` where `n` is a `COUNT(*)` alias) — not source columns
//! — so they must NOT be collected here; doing so would ask a `Scan` for a
//! column ("n") that doesn't exist in the underlying file.
//!
//! If no node references any real column at all (e.g. a bare `SELECT
//! COUNT(*) FROM t` with no `WHERE`/`GROUP BY`), `required` stays empty and
//! `Scan.columns` is left as `Vec::new()` — which is already the "all
//! columns" default the SQL builder starts with, so this is a safe (if
//! unoptimized) fallback rather than a special case to handle.

use std::collections::BTreeSet;

use atlas_query::{AggExpr, Expr, LogicalPlan, ScanNode};

use crate::Rule;

pub struct ColumnPruningRule;

impl Rule for ColumnPruningRule {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut required = BTreeSet::new();
        collect_plan_columns(&plan, &mut required);
        rewrite_scan(plan, &required)
    }
}

fn collect_plan_columns(plan: &LogicalPlan, acc: &mut BTreeSet<String>) {
    match plan {
        LogicalPlan::Scan(_) => {}
        LogicalPlan::Filter(node) => {
            collect_expr_columns(&node.predicate, acc);
            collect_plan_columns(&node.input, acc);
        }
        LogicalPlan::Project(node) => {
            for e in &node.exprs {
                collect_expr_columns(e, acc);
            }
            collect_plan_columns(&node.input, acc);
        }
        LogicalPlan::Aggregate(node) => {
            for e in &node.group_by {
                collect_expr_columns(e, acc);
            }
            for agg in &node.aggregates {
                collect_agg_columns(agg, acc);
            }
            collect_plan_columns(&node.input, acc);
        }
        // Sort/Limit are transparent to the source column namespace — see
        // module doc comment. Recurse without collecting.
        LogicalPlan::Sort(node) => collect_plan_columns(&node.input, acc),
        LogicalPlan::Limit(node) => collect_plan_columns(&node.input, acc),
    }
}

fn collect_agg_columns(agg: &AggExpr, acc: &mut BTreeSet<String>) {
    if let Some(arg) = &agg.arg {
        collect_expr_columns(arg, acc);
    }
}

fn collect_expr_columns(expr: &Expr, acc: &mut BTreeSet<String>) {
    match expr {
        Expr::Column(name) => {
            acc.insert(name.clone());
        }
        Expr::Literal(_) => {}
        Expr::Binary { left, right, .. } => {
            collect_expr_columns(left, acc);
            collect_expr_columns(right, acc);
        }
    }
}

fn rewrite_scan(plan: LogicalPlan, required: &BTreeSet<String>) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(node) => LogicalPlan::Scan(ScanNode {
            columns: required.iter().cloned().collect(),
            ..node
        }),
        LogicalPlan::Filter(mut node) => {
            node.input = Box::new(rewrite_scan(*node.input, required));
            LogicalPlan::Filter(node)
        }
        LogicalPlan::Project(mut node) => {
            node.input = Box::new(rewrite_scan(*node.input, required));
            LogicalPlan::Project(node)
        }
        LogicalPlan::Aggregate(mut node) => {
            node.input = Box::new(rewrite_scan(*node.input, required));
            LogicalPlan::Aggregate(node)
        }
        LogicalPlan::Sort(mut node) => {
            node.input = Box::new(rewrite_scan(*node.input, required));
            LogicalPlan::Sort(node)
        }
        LogicalPlan::Limit(mut node) => {
            node.input = Box::new(rewrite_scan(*node.input, required));
            LogicalPlan::Limit(node)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlas_query::{BinaryOp, FilterNode, Literal, ScanNode};

    fn scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            dataset: "t".into(),
            columns: Vec::new(),
            snapshot_id: String::new(),
        })
    }

    #[test]
    fn prunes_to_referenced_columns_only() {
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan()),
            predicate: Expr::Binary {
                left: Box::new(Expr::Column("age".into())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Literal::Int(50))),
            },
        });
        let pruned = ColumnPruningRule.apply(plan);
        let LogicalPlan::Filter(f) = pruned else {
            panic!("expected Filter");
        };
        let LogicalPlan::Scan(s) = *f.input else {
            panic!("expected Scan");
        };
        assert_eq!(s.columns, vec!["age".to_string()]);
    }

    #[test]
    fn no_referenced_columns_leaves_scan_empty() {
        let pruned = ColumnPruningRule.apply(scan());
        let LogicalPlan::Scan(s) = pruned else {
            panic!("expected Scan");
        };
        assert!(s.columns.is_empty());
    }

    #[test]
    fn order_by_aggregate_alias_does_not_leak_into_scan_columns() {
        use atlas_query::{AggExpr, AggFunc, AggregateNode, SortKey, SortNode};

        // SELECT diagnosis, COUNT(*) AS n FROM t GROUP BY diagnosis ORDER BY n
        let plan = LogicalPlan::Sort(SortNode {
            input: Box::new(LogicalPlan::Aggregate(AggregateNode {
                input: Box::new(scan()),
                group_by: vec![Expr::Column("diagnosis".into())],
                aggregates: vec![AggExpr {
                    func: AggFunc::Count,
                    arg: None,
                    alias: "n".into(),
                }],
            })),
            keys: vec![SortKey {
                expr: Expr::Column("n".into()),
                descending: true,
            }],
        });

        let pruned = ColumnPruningRule.apply(plan);
        let LogicalPlan::Sort(sort) = pruned else {
            panic!("expected Sort");
        };
        let LogicalPlan::Aggregate(agg) = *sort.input else {
            panic!("expected Aggregate");
        };
        let LogicalPlan::Scan(s) = *agg.input else {
            panic!("expected Scan");
        };
        // "n" is the COUNT(*) alias, not a source column — must not appear.
        assert_eq!(s.columns, vec!["diagnosis".to_string()]);
    }
}
