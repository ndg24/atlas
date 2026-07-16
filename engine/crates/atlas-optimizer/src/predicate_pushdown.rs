//! Pushes `Filter` closer to `Scan` by swapping `Filter(Project(inner))`
//! into `Project(Filter(inner))` wherever it appears. `atlas_query`'s SQL
//! builder already emits filter-closest-to-scan for every query shape it
//! can produce today (`Filter` always wraps `Scan` directly) — this rule
//! exists for plan shapes that don't have that property, e.g. Phase 6's
//! NL-compiled plans, and is exercised directly against hand-built
//! `LogicalPlan` values in the tests below since SQL alone can't produce
//! the "before" shape.
//!
//! A single pass only sinks one `Filter` past one `Project`; stacked
//! `Filter(Filter(Project(...)))` shapes fully normalize after this rule
//! runs again in `optimize`'s fixed-point loop.

use atlas_query::{FilterNode, LogicalPlan, ProjectNode};

use crate::Rule;

pub struct PredicatePushdownRule;

impl Rule for PredicatePushdownRule {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        rewrite(plan)
    }
}

fn rewrite(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(filter) => {
            let FilterNode { input, predicate } = filter;
            match *input {
                LogicalPlan::Project(project) => {
                    let ProjectNode {
                        input: inner,
                        exprs,
                        aliases,
                    } = project;
                    let pushed_filter = LogicalPlan::Filter(FilterNode {
                        input: Box::new(rewrite(*inner)),
                        predicate,
                    });
                    LogicalPlan::Project(ProjectNode {
                        input: Box::new(pushed_filter),
                        exprs,
                        aliases,
                    })
                }
                other => LogicalPlan::Filter(FilterNode {
                    input: Box::new(rewrite(other)),
                    predicate,
                }),
            }
        }
        LogicalPlan::Project(mut node) => {
            node.input = Box::new(rewrite(*node.input));
            LogicalPlan::Project(node)
        }
        LogicalPlan::Aggregate(mut node) => {
            node.input = Box::new(rewrite(*node.input));
            LogicalPlan::Aggregate(node)
        }
        LogicalPlan::Sort(mut node) => {
            node.input = Box::new(rewrite(*node.input));
            LogicalPlan::Sort(node)
        }
        LogicalPlan::Limit(mut node) => {
            node.input = Box::new(rewrite(*node.input));
            LogicalPlan::Limit(node)
        }
        LogicalPlan::Scan(_) => plan,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlas_query::{BinaryOp, Expr, Literal, ScanNode};

    fn scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            dataset: "t".into(),
            columns: Vec::new(),
            snapshot_id: String::new(),
        })
    }

    fn predicate() -> Expr {
        Expr::Binary {
            left: Box::new(Expr::Column("age".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(50))),
        }
    }

    fn project(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            exprs: vec![Expr::Column("diagnosis".into())],
            aliases: vec!["diagnosis".into()],
        })
    }

    #[test]
    fn filter_over_project_is_pushed_below_project() {
        let before = LogicalPlan::Filter(FilterNode {
            input: Box::new(project(scan())),
            predicate: predicate(),
        });
        let after = PredicatePushdownRule.apply(before);

        let LogicalPlan::Project(p) = &after else {
            panic!("expected Project at root, got {after:?}");
        };
        let LogicalPlan::Filter(f) = p.input.as_ref() else {
            panic!("expected Filter under Project, got {:?}", p.input);
        };
        assert_eq!(f.predicate, predicate());
        assert!(matches!(*f.input, LogicalPlan::Scan(_)));
    }

    #[test]
    fn project_over_filter_is_left_unchanged() {
        let before = project(LogicalPlan::Filter(FilterNode {
            input: Box::new(scan()),
            predicate: predicate(),
        }));
        let after = PredicatePushdownRule.apply(before.clone());
        assert_eq!(after, before);
    }

    #[test]
    fn both_orderings_normalize_to_the_same_tree() {
        let filter_over_project = LogicalPlan::Filter(FilterNode {
            input: Box::new(project(scan())),
            predicate: predicate(),
        });
        let project_over_filter = project(LogicalPlan::Filter(FilterNode {
            input: Box::new(scan()),
            predicate: predicate(),
        }));

        let a = PredicatePushdownRule.apply(filter_over_project);
        let b = PredicatePushdownRule.apply(project_over_filter);
        assert_eq!(a, b);
    }
}
