//! Per-column [`Statistics`] (min/max/null-count) computed while writing a
//! `.atlas` file, used later for predicate/partition pruning (Phase 4).

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
};
use arrow::compute;
use arrow::compute::concat;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::footer::Statistics;
use crate::hll::HyperLogLog;

/// Per-column [`Statistics`] computed directly from a set of in-memory
/// batches — the same concat-then-`compute_statistics` step
/// [`crate::writer::write_atlas`] does per column while writing a `.atlas`
/// file, but usable independently of that writer (e.g. for Parquet ingest,
/// which has no footer of its own to read stats back from).
pub fn compute_batches_column_stats(batches: &[RecordBatch]) -> Result<Vec<(String, Statistics)>> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .context("compute_batches_column_stats requires at least one batch")?;

    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, field)| {
            let parts: Vec<ArrayRef> = batches.iter().map(|b| b.column(col_idx).clone()).collect();
            let part_refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
            let column = concat(&part_refs).context("concatenating column across batches")?;
            Ok((field.name().clone(), compute_statistics(column.as_ref())))
        })
        .collect()
}

pub fn compute_statistics(array: &dyn Array) -> Statistics {
    let null_count = array.null_count() as u64;
    let (min, max) = match array.data_type() {
        DataType::Int64 => {
            let a = downcast::<Int64Array>(array);
            (le_bytes(compute::min(a)), le_bytes(compute::max(a)))
        }
        DataType::Float64 => {
            let a = downcast::<Float64Array>(array);
            (le_bytes(compute::min(a)), le_bytes(compute::max(a)))
        }
        DataType::Utf8 => {
            let a = downcast::<StringArray>(array);
            (
                compute::min_string(a)
                    .map(|v| v.as_bytes().to_vec())
                    .unwrap_or_default(),
                compute::max_string(a)
                    .map(|v| v.as_bytes().to_vec())
                    .unwrap_or_default(),
            )
        }
        DataType::Boolean => {
            let a = downcast::<BooleanArray>(array);
            (
                compute::min_boolean(a)
                    .map(|v| vec![v as u8])
                    .unwrap_or_default(),
                compute::max_boolean(a)
                    .map(|v| vec![v as u8])
                    .unwrap_or_default(),
            )
        }
        DataType::Date32 => {
            let a = downcast::<Date32Array>(array);
            (le_bytes(compute::min(a)), le_bytes(compute::max(a)))
        }
        other => panic!("unsupported column type for statistics: {other:?}"),
    };
    Statistics {
        min,
        max,
        null_count,
        distinct_count_estimate: estimate_distinct_count(array),
    }
}

/// Approximate distinct-value count for a column via HyperLogLog. Only the
/// final estimate is kept (not the sketch), so combining this across a
/// dataset's manifests — done at the coordinator, not here — is a capped sum
/// of per-file estimates, not an exact sketch union.
fn estimate_distinct_count(array: &dyn Array) -> u64 {
    let mut hll = HyperLogLog::new();
    match array.data_type() {
        DataType::Int64 => {
            let a = downcast::<Int64Array>(array);
            for i in 0..a.len() {
                if !a.is_null(i) {
                    hll.insert(&a.value(i).to_le_bytes());
                }
            }
        }
        DataType::Float64 => {
            let a = downcast::<Float64Array>(array);
            for i in 0..a.len() {
                if !a.is_null(i) {
                    hll.insert(&a.value(i).to_le_bytes());
                }
            }
        }
        DataType::Utf8 => {
            let a = downcast::<StringArray>(array);
            for i in 0..a.len() {
                if !a.is_null(i) {
                    hll.insert(a.value(i).as_bytes());
                }
            }
        }
        DataType::Boolean => {
            let a = downcast::<BooleanArray>(array);
            for i in 0..a.len() {
                if !a.is_null(i) {
                    hll.insert(&[a.value(i) as u8]);
                }
            }
        }
        DataType::Date32 => {
            let a = downcast::<Date32Array>(array);
            for i in 0..a.len() {
                if !a.is_null(i) {
                    hll.insert(&a.value(i).to_le_bytes());
                }
            }
        }
        _ => {}
    }
    hll.estimate()
}

fn downcast<T: 'static>(array: &dyn Array) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .expect("array data_type matched downcast target")
}

fn le_bytes<N: LeBytes>(value: Option<N>) -> Vec<u8> {
    value.map(|v| v.to_le_bytes_vec()).unwrap_or_default()
}

trait LeBytes {
    fn to_le_bytes_vec(&self) -> Vec<u8>;
}

impl LeBytes for i64 {
    fn to_le_bytes_vec(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl LeBytes for i32 {
    fn to_le_bytes_vec(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl LeBytes for f64 {
    fn to_le_bytes_vec(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_count_estimate_close_to_true_cardinality() {
        let values: Vec<i64> = (0..5000).map(|i| i % 500).collect(); // 500 distinct
        let array = Int64Array::from(values);
        let stats = compute_statistics(&array);
        let error = (stats.distinct_count_estimate as f64 - 500.0).abs() / 500.0;
        assert!(
            error < 0.2,
            "distinct_count_estimate {} too far from true 500",
            stats.distinct_count_estimate
        );
    }

    #[test]
    fn distinct_count_estimate_ignores_nulls() {
        let array = Int64Array::from(vec![Some(1), None, Some(1), None, Some(2)]);
        let stats = compute_statistics(&array);
        assert_eq!(stats.null_count, 2);
        assert!(stats.distinct_count_estimate <= 3);
    }
}
