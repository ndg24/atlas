use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};

use atlas_query::{AggExpr, AggFunc, Expr};

use crate::expr::eval_expr;
use crate::scalar::{scalar_at, ScalarValue};

#[derive(Clone)]
enum Accumulator {
    CountStar(i64),
    CountArg(i64),
    SumInt(i64),
    SumFloat(f64),
    AvgInt { sum: i64, count: i64 },
    AvgFloat { sum: f64, count: i64 },
    MinInt(Option<i64>),
    MinFloat(Option<f64>),
    MinStr(Option<String>),
    MaxInt(Option<i64>),
    MaxFloat(Option<f64>),
    MaxStr(Option<String>),
}

impl Accumulator {
    fn new(func: AggFunc, arg_type: Option<&DataType>) -> Result<Self> {
        use AggFunc::*;
        Ok(match (func, arg_type) {
            (Count, None) => Accumulator::CountStar(0),
            (Count, Some(_)) => Accumulator::CountArg(0),
            (Sum, Some(DataType::Int64)) => Accumulator::SumInt(0),
            (Sum, Some(DataType::Float64)) => Accumulator::SumFloat(0.0),
            (Avg, Some(DataType::Int64)) => Accumulator::AvgInt { sum: 0, count: 0 },
            (Avg, Some(DataType::Float64)) => Accumulator::AvgFloat { sum: 0.0, count: 0 },
            (Min, Some(DataType::Int64)) => Accumulator::MinInt(None),
            (Min, Some(DataType::Float64)) => Accumulator::MinFloat(None),
            (Min, Some(DataType::Utf8)) => Accumulator::MinStr(None),
            (Max, Some(DataType::Int64)) => Accumulator::MaxInt(None),
            (Max, Some(DataType::Float64)) => Accumulator::MaxFloat(None),
            (Max, Some(DataType::Utf8)) => Accumulator::MaxStr(None),
            (f, t) => bail!("unsupported aggregate {f:?} over argument type {t:?}"),
        })
    }

    fn output_type(&self) -> DataType {
        match self {
            Accumulator::CountStar(_) | Accumulator::CountArg(_) => DataType::Int64,
            Accumulator::SumInt(_) | Accumulator::MinInt(_) | Accumulator::MaxInt(_) => {
                DataType::Int64
            }
            Accumulator::SumFloat(_)
            | Accumulator::AvgInt { .. }
            | Accumulator::AvgFloat { .. }
            | Accumulator::MinFloat(_)
            | Accumulator::MaxFloat(_) => DataType::Float64,
            Accumulator::MinStr(_) | Accumulator::MaxStr(_) => DataType::Utf8,
        }
    }

    fn update(&mut self, value: &ScalarValue) {
        match self {
            Accumulator::CountStar(c) => *c += 1,
            Accumulator::CountArg(c) => {
                if !matches!(value, ScalarValue::Null) {
                    *c += 1;
                }
            }
            Accumulator::SumInt(s) => {
                if let ScalarValue::Int64(i) = value {
                    *s += i;
                }
            }
            Accumulator::SumFloat(s) => {
                if let ScalarValue::Float64(f) = value {
                    *s += f;
                }
            }
            Accumulator::AvgInt { sum, count } => {
                if let ScalarValue::Int64(i) = value {
                    *sum += i;
                    *count += 1;
                }
            }
            Accumulator::AvgFloat { sum, count } => {
                if let ScalarValue::Float64(f) = value {
                    *sum += f;
                    *count += 1;
                }
            }
            Accumulator::MinInt(m) => {
                if let ScalarValue::Int64(i) = value {
                    *m = Some(m.map_or(*i, |cur| cur.min(*i)));
                }
            }
            Accumulator::MinFloat(m) => {
                if let ScalarValue::Float64(f) = value {
                    *m = Some(m.map_or(*f, |cur| cur.min(*f)));
                }
            }
            Accumulator::MinStr(m) => {
                if let ScalarValue::Utf8(s) = value {
                    if m.as_ref().is_none_or(|cur| s < cur) {
                        *m = Some(s.clone());
                    }
                }
            }
            Accumulator::MaxInt(m) => {
                if let ScalarValue::Int64(i) = value {
                    *m = Some(m.map_or(*i, |cur| cur.max(*i)));
                }
            }
            Accumulator::MaxFloat(m) => {
                if let ScalarValue::Float64(f) = value {
                    *m = Some(m.map_or(*f, |cur| cur.max(*f)));
                }
            }
            Accumulator::MaxStr(m) => {
                if let ScalarValue::Utf8(s) = value {
                    if m.as_ref().is_none_or(|cur| s > cur) {
                        *m = Some(s.clone());
                    }
                }
            }
        }
    }

    fn finish(self) -> ScalarValue {
        match self {
            Accumulator::CountStar(c) | Accumulator::CountArg(c) => ScalarValue::Int64(c),
            Accumulator::SumInt(s) => ScalarValue::Int64(s),
            Accumulator::SumFloat(s) => ScalarValue::Float64(s),
            Accumulator::AvgInt { sum, count } => ScalarValue::Float64(if count == 0 {
                0.0
            } else {
                sum as f64 / count as f64
            }),
            Accumulator::AvgFloat { sum, count } => {
                ScalarValue::Float64(if count == 0 { 0.0 } else { sum / count as f64 })
            }
            Accumulator::MinInt(m) | Accumulator::MaxInt(m) => {
                m.map(ScalarValue::Int64).unwrap_or(ScalarValue::Null)
            }
            Accumulator::MinFloat(m) | Accumulator::MaxFloat(m) => {
                m.map(ScalarValue::Float64).unwrap_or(ScalarValue::Null)
            }
            Accumulator::MinStr(m) | Accumulator::MaxStr(m) => {
                m.map(ScalarValue::Utf8).unwrap_or(ScalarValue::Null)
            }
        }
    }
}

fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::Literal(_) => "literal".to_string(),
        Expr::Binary { .. } => "expr".to_string(),
    }
}

fn build_array(data_type: &DataType, values: &[ScalarValue]) -> Result<ArrayRef> {
    Ok(match data_type {
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for v in values {
                match v {
                    ScalarValue::Int64(i) => b.append_value(*i),
                    ScalarValue::Null => b.append_null(),
                    other => bail!("expected Int64 value, got {other:?}"),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for v in values {
                match v {
                    ScalarValue::Float64(f) => b.append_value(*f),
                    ScalarValue::Null => b.append_null(),
                    other => bail!("expected Float64 value, got {other:?}"),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for v in values {
                match v {
                    ScalarValue::Utf8(s) => b.append_value(s),
                    ScalarValue::Null => b.append_null(),
                    other => bail!("expected Utf8 value, got {other:?}"),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for v in values {
                match v {
                    ScalarValue::Boolean(bv) => b.append_value(*bv),
                    ScalarValue::Null => b.append_null(),
                    other => bail!("expected Boolean value, got {other:?}"),
                }
            }
            Arc::new(b.finish())
        }
        other => bail!("unsupported aggregate output type: {other:?}"),
    })
}

pub fn exec_aggregate(
    batches: &[RecordBatch],
    group_by: &[Expr],
    aggregates: &[AggExpr],
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    let schema = batches[0].schema();
    let batch = concat_batches(&schema, batches)?;
    let num_rows = batch.num_rows();

    let group_arrays: Vec<ArrayRef> = group_by
        .iter()
        .map(|e| eval_expr(&batch, e))
        .collect::<Result<_>>()?;
    let group_types: Vec<DataType> = group_arrays.iter().map(|a| a.data_type().clone()).collect();
    let group_names: Vec<String> = group_by.iter().map(expr_display_name).collect();

    let agg_arg_arrays: Vec<Option<ArrayRef>> = aggregates
        .iter()
        .map(|a| a.arg.as_ref().map(|e| eval_expr(&batch, e)).transpose())
        .collect::<Result<_>>()?;
    let agg_arg_types: Vec<Option<DataType>> = agg_arg_arrays
        .iter()
        .map(|a| a.as_ref().map(|arr| arr.data_type().clone()))
        .collect();

    let mut groups: HashMap<Vec<ScalarValue>, Vec<Accumulator>> = HashMap::new();
    for row in 0..num_rows {
        let key: Vec<ScalarValue> = group_arrays
            .iter()
            .map(|a| scalar_at(a, row))
            .collect::<Result<_>>()?;

        let entry = match groups.get_mut(&key) {
            Some(accs) => accs,
            None => {
                let accs = aggregates
                    .iter()
                    .zip(&agg_arg_types)
                    .map(|(agg, arg_type)| Accumulator::new(agg.func, arg_type.as_ref()))
                    .collect::<Result<Vec<_>>>()?;
                groups.entry(key.clone()).or_insert(accs)
            }
        };

        for (acc, arg_array) in entry.iter_mut().zip(&agg_arg_arrays) {
            let value = match arg_array {
                Some(arr) => scalar_at(arr, row)?,
                None => ScalarValue::Null, // COUNT(*) ignores the value entirely
            };
            acc.update(&value);
        }
    }

    let mut entries: Vec<(Vec<ScalarValue>, Vec<Accumulator>)> = groups.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| format!("{a:?}").cmp(&format!("{b:?}")));

    let mut group_columns: Vec<Vec<ScalarValue>> =
        vec![Vec::with_capacity(entries.len()); group_by.len()];
    let mut agg_columns: Vec<Vec<ScalarValue>> =
        vec![Vec::with_capacity(entries.len()); aggregates.len()];
    for (key, accs) in entries {
        for (col, value) in group_columns.iter_mut().zip(key) {
            col.push(value);
        }
        for (col, acc) in agg_columns.iter_mut().zip(accs) {
            col.push(acc.finish());
        }
    }

    let mut fields = Vec::with_capacity(group_by.len() + aggregates.len());
    let mut arrays = Vec::with_capacity(group_by.len() + aggregates.len());
    for ((name, data_type), values) in group_names.iter().zip(&group_types).zip(&group_columns) {
        fields.push(Field::new(name, data_type.clone(), true));
        arrays.push(build_array(data_type, values)?);
    }
    for (i, (agg, values)) in aggregates.iter().zip(&agg_columns).enumerate() {
        let output_type = Accumulator::new(agg.func, agg_arg_types[i].as_ref())?.output_type();
        fields.push(Field::new(&agg.alias, output_type.clone(), true));
        arrays.push(build_array(&output_type, values)?);
    }

    Ok(vec![RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        arrays,
    )?])
}
