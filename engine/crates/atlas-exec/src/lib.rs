//! Logical plan execution for Atlas: one function per node type, plus
//! `execute` which walks a plan tree bottom-up dispatching to them.

mod aggregate;
mod expr;
mod ops;
mod scalar;

pub use aggregate::exec_aggregate;
pub use ops::*;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    use atlas_query::{AggExpr, AggFunc, BinaryOp, Expr, Literal, SortKey};

    fn patients_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("diagnosis", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("cost", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["flu", "flu", "cold", "cold", "flu"])),
                Arc::new(Int64Array::from(vec![60, 70, 20, 80, 30])),
                Arc::new(Float64Array::from(vec![100.0, 200.0, 50.0, 150.0, 75.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn filter_keeps_matching_rows() {
        let batches = vec![patients_batch()];
        let predicate = Expr::Binary {
            left: Box::new(Expr::Column("age".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(50))),
        };
        let result = exec_filter(&batches, &predicate).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // ages 60, 70, 80
    }

    #[test]
    fn group_by_count() {
        let batches = vec![patients_batch()];
        let group_by = vec![Expr::Column("diagnosis".into())];
        let aggregates = vec![AggExpr {
            func: AggFunc::Count,
            arg: None,
            alias: "n".into(),
        }];
        let result = exec_aggregate(&batches, &group_by, &aggregates).unwrap();
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2); // "cold" and "flu"

        let diagnosis = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut pairs: Vec<(&str, i64)> = (0..batch.num_rows())
            .map(|i| (diagnosis.value(i), counts.value(i)))
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec![("cold", 2), ("flu", 3)]);
    }

    #[test]
    fn group_by_sum() {
        let batches = vec![patients_batch()];
        let group_by = vec![Expr::Column("diagnosis".into())];
        let aggregates = vec![AggExpr {
            func: AggFunc::Sum,
            arg: Some(Expr::Column("cost".into())),
            alias: "total".into(),
        }];
        let result = exec_aggregate(&batches, &group_by, &aggregates).unwrap();
        let batch = &result[0];
        let diagnosis = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let totals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut pairs: Vec<(&str, f64)> = (0..batch.num_rows())
            .map(|i| (diagnosis.value(i), totals.value(i)))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(pairs, vec![("cold", 200.0), ("flu", 375.0)]);
    }

    #[test]
    fn sort_then_limit() {
        let batches = vec![patients_batch()];
        let keys = vec![SortKey {
            expr: Expr::Column("age".into()),
            descending: true,
        }];
        let sorted = exec_sort(&batches, &keys).unwrap();
        let limited = exec_limit(&sorted, 2).unwrap();
        let total_rows: usize = limited.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        let ages = limited[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ages.values(), &[80, 70]);
    }
}
