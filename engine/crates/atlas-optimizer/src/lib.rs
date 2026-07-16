//! Rule-based logical-plan optimizer for Atlas (Phase 4).
//!
//! Rules operate purely on `atlas_query::LogicalPlan`/`Expr` trees — no
//! Arrow/exec dependency, since a plan can be rewritten before any data is
//! touched. `optimize` applies the fixed rule set to a fixed point (or a
//! hard iteration cap, to guarantee termination even if a future rule ever
//! introduces an oscillation).

mod column_pruning;
mod predicate_pushdown;

pub use column_pruning::ColumnPruningRule;
pub use predicate_pushdown::PredicatePushdownRule;

use atlas_query::LogicalPlan;

pub trait Rule {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}

const MAX_ITERATIONS: usize = 20;

pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    let rules: Vec<Box<dyn Rule>> =
        vec![Box::new(ColumnPruningRule), Box::new(PredicatePushdownRule)];
    optimize_with_rules(plan, &rules)
}

pub fn optimize_with_rules(mut plan: LogicalPlan, rules: &[Box<dyn Rule>]) -> LogicalPlan {
    for _ in 0..MAX_ITERATIONS {
        let before = plan.clone();
        for rule in rules {
            plan = rule.apply(plan);
        }
        if plan == before {
            break;
        }
    }
    plan
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
    fn optimize_terminates_and_is_idempotent() {
        let plan = plan_for(
            "SELECT diagnosis, COUNT(*) AS n FROM t WHERE age > 50 GROUP BY diagnosis ORDER BY n DESC LIMIT 5",
        );
        let optimized = optimize(plan.clone());
        let optimized_again = optimize(optimized.clone());
        assert_eq!(optimized, optimized_again, "optimize should be idempotent");
    }

    #[test]
    fn optimize_prunes_columns_on_a_realistic_query() {
        let plan = plan_for("SELECT diagnosis FROM t WHERE age > 50");
        let optimized = optimize(plan);

        let LogicalPlan::Project(project) = &optimized else {
            panic!("expected Project at root, got {optimized:?}");
        };
        let LogicalPlan::Filter(filter) = project.input.as_ref() else {
            panic!("expected Filter under Project, got {:?}", project.input);
        };
        let LogicalPlan::Scan(scan) = filter.input.as_ref() else {
            panic!("expected Scan under Filter, got {:?}", filter.input);
        };
        assert_eq!(
            scan.columns,
            vec!["age".to_string(), "diagnosis".to_string()]
        );
    }
}
