//! SQL parsing and logical-plan construction for Atlas.

pub mod plan;
mod sql;

pub use plan::*;
pub use sql::{build_logical_plan, parse_sql};

#[cfg(test)]
mod tests {
    use super::*;
    use atlas_format::{DataType, Field, Schema};

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("diagnosis", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("cost", DataType::Float64, false),
        ])
    }

    fn plan_for(sql: &str) -> LogicalPlan {
        let stmt = parse_sql(sql).unwrap();
        build_logical_plan(&stmt, &test_schema()).unwrap()
    }

    #[test]
    fn simple_select_with_where() {
        let plan = plan_for("SELECT diagnosis, age FROM t WHERE age > 50");
        match plan {
            LogicalPlan::Project(p) => match *p.input {
                LogicalPlan::Filter(f) => {
                    assert_eq!(
                        f.predicate,
                        Expr::Binary {
                            left: Box::new(Expr::Column("age".into())),
                            op: BinaryOp::Gt,
                            right: Box::new(Expr::Literal(Literal::Int(50))),
                        }
                    );
                    assert!(matches!(*f.input, LogicalPlan::Scan(_)));
                }
                other => panic!("expected Filter under Project, got {other:?}"),
            },
            other => panic!("expected Project at root, got {other:?}"),
        }
    }

    #[test]
    fn group_by_count_with_order_and_limit() {
        let plan = plan_for(
            "SELECT diagnosis, COUNT(*) AS n FROM t WHERE age > 50 GROUP BY diagnosis ORDER BY n DESC LIMIT 5",
        );
        let LogicalPlan::Limit(limit) = plan else {
            panic!("expected Limit at root");
        };
        assert_eq!(limit.n, 5);
        let LogicalPlan::Sort(sort) = *limit.input else {
            panic!("expected Sort under Limit");
        };
        assert_eq!(sort.keys.len(), 1);
        assert!(sort.keys[0].descending);
        assert_eq!(sort.keys[0].expr, Expr::Column("n".into()));

        let LogicalPlan::Aggregate(agg) = *sort.input else {
            panic!("expected Aggregate under Sort");
        };
        assert_eq!(agg.group_by, vec![Expr::Column("diagnosis".into())]);
        assert_eq!(agg.aggregates.len(), 1);
        assert_eq!(agg.aggregates[0].func, AggFunc::Count);
        assert_eq!(agg.aggregates[0].arg, None);
        assert_eq!(agg.aggregates[0].alias, "n");

        assert!(matches!(*agg.input, LogicalPlan::Filter(_)));
    }

    #[test]
    fn all_aggregate_functions_parse() {
        let plan = plan_for(
            "SELECT diagnosis, COUNT(*) AS c, SUM(cost) AS s, AVG(cost) AS a, MIN(cost) AS mn, MAX(cost) AS mx FROM t GROUP BY diagnosis",
        );
        let LogicalPlan::Aggregate(agg) = plan else {
            panic!("expected Aggregate at root");
        };
        let funcs: Vec<AggFunc> = agg.aggregates.iter().map(|a| a.func).collect();
        assert_eq!(
            funcs,
            vec![
                AggFunc::Count,
                AggFunc::Sum,
                AggFunc::Avg,
                AggFunc::Min,
                AggFunc::Max
            ]
        );
    }

    #[test]
    fn and_or_predicate() {
        let plan = plan_for("SELECT diagnosis FROM t WHERE age > 50 AND cost < 100.0");
        let LogicalPlan::Project(p) = plan else {
            panic!("expected Project at root");
        };
        let LogicalPlan::Filter(f) = *p.input else {
            panic!("expected Filter under Project");
        };
        assert!(matches!(
            f.predicate,
            Expr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
    }
}
