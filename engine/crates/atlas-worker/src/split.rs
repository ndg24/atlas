//! Splits a compiled [`LogicalPlan`] into the two halves distributed
//! execution needs (see `proto/worker.proto` for the wire-level rationale):
//!
//! - `partial`: run once per manifest/file, against that partition's rows.
//! - `combine`: run once, over the union of every partial result — the
//!   two-phase aggregate re-combine and/or the final `ORDER BY` + `LIMIT`.
//!   `None` when the query is a plain scan/filter/project with neither an
//!   aggregate nor a sort/limit, in which case the coordinator can just
//!   concatenate worker outputs directly.

use atlas_query::{
    AggExpr, AggFunc, AggregateNode, BinaryOp, Expr, LimitNode, Literal, LogicalPlan, ProjectNode,
    ScanNode, SortKey, SortNode,
};

pub struct SplitPlan {
    pub partial: LogicalPlan,
    pub combine: Option<LogicalPlan>,
}

/// `atlas_exec::exec_scan` returns its input batches unchanged regardless of
/// what's inside `ScanNode` — so this placeholder, used as the leaf of a
/// combine plan, just means "whatever rows were handed to `execute`" (the
/// unioned partial results).
fn placeholder_scan() -> LogicalPlan {
    LogicalPlan::Scan(ScanNode {
        dataset: String::new(),
        columns: Vec::new(),
        snapshot_id: String::new(),
    })
}

enum TopWrapper {
    Sort(Vec<SortKey>),
    Limit(u64),
}

impl TopWrapper {
    fn rewrap(self, input: LogicalPlan) -> LogicalPlan {
        match self {
            TopWrapper::Sort(keys) => LogicalPlan::Sort(SortNode {
                input: Box::new(input),
                keys,
            }),
            TopWrapper::Limit(n) => LogicalPlan::Limit(LimitNode {
                input: Box::new(input),
                n,
            }),
        }
    }
}

/// Strips `Sort`/`Limit` nodes off the root, innermost-last, since both
/// require the full (combined) result set to be correct — pushing them down
/// to per-partition execution would sort/limit each partition independently,
/// which is not the same as sorting/limiting the whole result.
fn peel_top(mut plan: LogicalPlan) -> (Vec<TopWrapper>, LogicalPlan) {
    let mut wrappers = Vec::new();
    loop {
        plan = match plan {
            LogicalPlan::Sort(node) => {
                wrappers.push(TopWrapper::Sort(node.keys));
                *node.input
            }
            LogicalPlan::Limit(node) => {
                wrappers.push(TopWrapper::Limit(node.n));
                *node.input
            }
            other => return (wrappers, other),
        };
    }
}

/// Reapply peeled wrappers in their original nesting order: `wrappers` was
/// built outermost-first, so rebuilding outermost-last (`.rev()`) restores
/// e.g. `Limit(Sort(core))` from `[Limit, Sort]`.
fn rewrap(wrappers: Vec<TopWrapper>, core: LogicalPlan) -> LogicalPlan {
    wrappers
        .into_iter()
        .rev()
        .fold(core, |acc, w| w.rewrap(acc))
}

fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::Literal(_) => "literal".to_string(),
        Expr::Binary { .. } => "expr".to_string(),
    }
}

/// One original `AggExpr` decomposes into one (COUNT/SUM/MIN/MAX) or two
/// (AVG -> sum+count) partial `AggExpr`s, plus the combine-side `AggExpr`(s)
/// that re-aggregate the partials back into the same final `alias`.
struct AggSplit {
    partial: Vec<AggExpr>,
    combine: Vec<AggExpr>,
    /// Set only for AVG: `(final_alias, sum_alias, count_alias)` — the
    /// combine `Aggregate` outputs `sum_alias`/`count_alias`; a `Project` on
    /// top divides them back into `final_alias`.
    avg_division: Option<(String, String, String)>,
}

/// COUNT of partial COUNTs must SUM (not COUNT again) to get the true total;
/// SUM/MIN/MAX of partial SUM/MIN/MAX combine with themselves; AVG can't
/// combine at all without first decomposing into SUM and COUNT, since the
/// average of per-partition averages isn't the overall average unless every
/// partition has the same row count.
fn split_agg_expr(agg: &AggExpr) -> AggSplit {
    match agg.func {
        AggFunc::Avg => {
            let sum_alias = format!("__avg_sum_{}", agg.alias);
            let count_alias = format!("__avg_count_{}", agg.alias);
            AggSplit {
                partial: vec![
                    AggExpr {
                        func: AggFunc::Sum,
                        arg: agg.arg.clone(),
                        alias: sum_alias.clone(),
                    },
                    AggExpr {
                        func: AggFunc::Count,
                        arg: agg.arg.clone(),
                        alias: count_alias.clone(),
                    },
                ],
                combine: vec![
                    AggExpr {
                        func: AggFunc::Sum,
                        arg: Some(Expr::Column(sum_alias.clone())),
                        alias: sum_alias.clone(),
                    },
                    AggExpr {
                        func: AggFunc::Sum,
                        arg: Some(Expr::Column(count_alias.clone())),
                        alias: count_alias.clone(),
                    },
                ],
                avg_division: Some((agg.alias.clone(), sum_alias, count_alias)),
            }
        }
        AggFunc::Count => AggSplit {
            partial: vec![agg.clone()],
            combine: vec![AggExpr {
                func: AggFunc::Sum,
                arg: Some(Expr::Column(agg.alias.clone())),
                alias: agg.alias.clone(),
            }],
            avg_division: None,
        },
        AggFunc::Sum | AggFunc::Min | AggFunc::Max => AggSplit {
            partial: vec![agg.clone()],
            combine: vec![AggExpr {
                func: agg.func,
                arg: Some(Expr::Column(agg.alias.clone())),
                alias: agg.alias.clone(),
            }],
            avg_division: None,
        },
    }
}

fn split_aggregate(node: AggregateNode) -> (LogicalPlan, LogicalPlan) {
    let AggregateNode {
        input,
        group_by,
        aggregates,
    } = node;

    let splits: Vec<AggSplit> = aggregates.iter().map(split_agg_expr).collect();
    let partial_aggs: Vec<AggExpr> = splits.iter().flat_map(|s| s.partial.clone()).collect();
    let combine_aggs: Vec<AggExpr> = splits.iter().flat_map(|s| s.combine.clone()).collect();

    let partial = LogicalPlan::Aggregate(AggregateNode {
        input,
        group_by: group_by.clone(),
        aggregates: partial_aggs,
    });

    let combine_aggregate = LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(placeholder_scan()),
        group_by: group_by.clone(),
        aggregates: combine_aggs,
    });

    let needs_projection = splits.iter().any(|s| s.avg_division.is_some());
    let combine = if needs_projection {
        let mut exprs = Vec::with_capacity(group_by.len() + aggregates.len());
        let mut aliases = Vec::with_capacity(group_by.len() + aggregates.len());
        for g in &group_by {
            exprs.push(g.clone());
            aliases.push(expr_display_name(g));
        }
        for (agg, split) in aggregates.iter().zip(&splits) {
            match &split.avg_division {
                // `sum + 0.0` forces the Int64/Float64 coercion `eval_binary`
                // already does for mismatched operand types, so the division
                // below runs in floating point even when the summed column
                // was Int64 — plain Int64/Int64 division would truncate.
                Some((final_alias, sum_alias, count_alias)) => {
                    exprs.push(Expr::Binary {
                        left: Box::new(Expr::Binary {
                            left: Box::new(Expr::Column(sum_alias.clone())),
                            op: BinaryOp::Add,
                            right: Box::new(Expr::Literal(Literal::Float(0.0))),
                        }),
                        op: BinaryOp::Div,
                        right: Box::new(Expr::Column(count_alias.clone())),
                    });
                    aliases.push(final_alias.clone());
                }
                None => {
                    exprs.push(Expr::Column(agg.alias.clone()));
                    aliases.push(agg.alias.clone());
                }
            }
        }
        LogicalPlan::Project(ProjectNode {
            input: Box::new(combine_aggregate),
            exprs,
            aliases,
        })
    } else {
        combine_aggregate
    };

    (partial, combine)
}

pub fn split_for_distribution(plan: LogicalPlan) -> SplitPlan {
    let (wrappers, core) = peel_top(plan);
    match core {
        LogicalPlan::Aggregate(node) => {
            let (partial, combine_core) = split_aggregate(node);
            SplitPlan {
                partial,
                combine: Some(rewrap(wrappers, combine_core)),
            }
        }
        other if wrappers.is_empty() => SplitPlan {
            partial: other,
            combine: None,
        },
        other => SplitPlan {
            partial: other,
            combine: Some(rewrap(wrappers, placeholder_scan())),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlas_format::{DataType, Field, Schema};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("diagnosis", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("cost", DataType::Float64, false),
        ])
    }

    fn plan_for(sql: &str) -> LogicalPlan {
        let stmt = atlas_query::parse_sql(sql).unwrap();
        atlas_query::build_logical_plan(&stmt, &schema()).unwrap()
    }

    #[test]
    fn plain_scan_filter_needs_no_combine() {
        let split = split_for_distribution(plan_for("SELECT diagnosis FROM t WHERE age > 50"));
        assert!(split.combine.is_none());
    }

    #[test]
    fn group_by_count_combines_via_sum() {
        let split = split_for_distribution(plan_for(
            "SELECT diagnosis, COUNT(*) AS n FROM t GROUP BY diagnosis",
        ));
        let LogicalPlan::Aggregate(partial_agg) = &split.partial else {
            panic!("expected partial Aggregate");
        };
        assert_eq!(partial_agg.aggregates[0].func, AggFunc::Count);

        let combine = split.combine.expect("combine plan expected");
        let LogicalPlan::Aggregate(combine_agg) = &combine else {
            panic!("expected combine Aggregate, got {combine:?}");
        };
        assert_eq!(combine_agg.aggregates[0].func, AggFunc::Sum);
        assert_eq!(combine_agg.aggregates[0].alias, "n");
    }

    #[test]
    fn avg_decomposes_into_sum_and_count() {
        let split = split_for_distribution(plan_for(
            "SELECT diagnosis, AVG(cost) AS avg_cost FROM t GROUP BY diagnosis",
        ));
        let LogicalPlan::Aggregate(partial_agg) = &split.partial else {
            panic!("expected partial Aggregate");
        };
        assert_eq!(partial_agg.aggregates.len(), 2);
        assert!(partial_agg
            .aggregates
            .iter()
            .any(|a| a.func == AggFunc::Sum && a.alias == "__avg_sum_avg_cost"));
        assert!(partial_agg
            .aggregates
            .iter()
            .any(|a| a.func == AggFunc::Count && a.alias == "__avg_count_avg_cost"));

        let combine = split.combine.expect("combine plan expected");
        let LogicalPlan::Project(project) = &combine else {
            panic!("expected combine Project wrapping the re-Aggregate, got {combine:?}");
        };
        assert_eq!(project.aliases.last().unwrap(), "avg_cost");
    }

    #[test]
    fn order_by_limit_without_aggregate_combines_over_placeholder_scan() {
        let split = split_for_distribution(plan_for(
            "SELECT diagnosis FROM t ORDER BY diagnosis LIMIT 5",
        ));
        assert!(matches!(split.partial, LogicalPlan::Project(_)));
        let combine = split.combine.expect("combine plan expected");
        let LogicalPlan::Limit(limit) = &combine else {
            panic!("expected Limit at combine root, got {combine:?}");
        };
        assert_eq!(limit.n, 5);
        assert!(matches!(*limit.input, LogicalPlan::Sort(_)));
    }
}
