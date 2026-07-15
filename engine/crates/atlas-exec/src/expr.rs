use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::kernels::boolean::{and, or};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::numeric::{add, div, mul, sub};
use arrow::record_batch::RecordBatch;

use atlas_query::{BinaryOp, Expr, Literal};

/// Evaluate an expression against a batch, producing one output value per
/// row. Literals are broadcast to the batch's row count.
pub fn eval_expr(batch: &RecordBatch, expr: &Expr) -> Result<ArrayRef> {
    match expr {
        Expr::Column(name) => batch
            .column_by_name(name)
            .cloned()
            .ok_or_else(|| anyhow!("unknown column: {name}")),
        Expr::Literal(lit) => Ok(literal_array(lit, batch.num_rows())),
        Expr::Binary { left, op, right } => {
            let l = eval_expr(batch, left)?;
            let r = eval_expr(batch, right)?;
            eval_binary(*op, &l, &r)
        }
    }
}

fn literal_array(lit: &Literal, len: usize) -> ArrayRef {
    match lit {
        Literal::Int(i) => Arc::new(Int64Array::from(vec![*i; len])),
        Literal::Float(f) => Arc::new(Float64Array::from(vec![*f; len])),
        Literal::Str(s) => Arc::new(StringArray::from(vec![s.clone(); len])),
        Literal::Bool(b) => Arc::new(BooleanArray::from(vec![*b; len])),
    }
}

fn eval_binary(op: BinaryOp, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    let (left, right) = coerce_numeric_pair(left, right)?;
    match op {
        BinaryOp::Eq => Ok(Arc::new(eq(&left, &right)?)),
        BinaryOp::NotEq => Ok(Arc::new(neq(&left, &right)?)),
        BinaryOp::Lt => Ok(Arc::new(lt(&left, &right)?)),
        BinaryOp::LtEq => Ok(Arc::new(lt_eq(&left, &right)?)),
        BinaryOp::Gt => Ok(Arc::new(gt(&left, &right)?)),
        BinaryOp::GtEq => Ok(Arc::new(gt_eq(&left, &right)?)),
        BinaryOp::And => Ok(Arc::new(and(as_bool(&left)?, as_bool(&right)?)?)),
        BinaryOp::Or => Ok(Arc::new(or(as_bool(&left)?, as_bool(&right)?)?)),
        BinaryOp::Add => Ok(add(&left, &right)?),
        BinaryOp::Sub => Ok(sub(&left, &right)?),
        BinaryOp::Mul => Ok(mul(&left, &right)?),
        BinaryOp::Div => Ok(div(&left, &right)?),
    }
}

fn as_bool(array: &ArrayRef) -> Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow!("expected boolean operand, got {:?}", array.data_type()))
}

/// If one side is Int64 and the other Float64 (e.g. comparing an integer
/// column against a float literal), widen the Int64 side to Float64 so
/// arrow's kernels see matching types on both sides.
fn coerce_numeric_pair(left: &ArrayRef, right: &ArrayRef) -> Result<(ArrayRef, ArrayRef)> {
    use arrow::datatypes::DataType;
    match (left.data_type(), right.data_type()) {
        (DataType::Int64, DataType::Float64) => Ok((cast_int64_to_float64(left)?, right.clone())),
        (DataType::Float64, DataType::Int64) => Ok((left.clone(), cast_int64_to_float64(right)?)),
        _ => Ok((left.clone(), right.clone())),
    }
}

fn cast_int64_to_float64(array: &ArrayRef) -> Result<ArrayRef> {
    arrow::compute::cast(array, &arrow::datatypes::DataType::Float64)
        .map_err(|e| anyhow!("failed to coerce Int64 to Float64: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("age", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 60, 30])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn column_lookup() {
        let b = batch();
        let result = eval_expr(&b, &Expr::Column("age".into())).unwrap();
        assert_eq!(
            result
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[10, 60, 30]
        );
    }

    #[test]
    fn gt_comparison() {
        let b = batch();
        let expr = Expr::Binary {
            left: Box::new(Expr::Column("age".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(50))),
        };
        let result = eval_expr(&b, &expr).unwrap();
        let bools = result.as_boolean();
        assert_eq!(
            bools.values().iter().collect::<Vec<_>>(),
            vec![false, true, false]
        );
    }

    #[test]
    fn and_combination() {
        let b = batch();
        let expr = Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(Expr::Column("age".into())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Literal::Int(5))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::Column("age".into())),
                op: BinaryOp::Lt,
                right: Box::new(Expr::Literal(Literal::Int(50))),
            }),
        };
        let result = eval_expr(&b, &expr).unwrap();
        let bools = result.as_boolean();
        assert_eq!(
            bools.values().iter().collect::<Vec<_>>(),
            vec![true, false, true]
        );
    }
}
