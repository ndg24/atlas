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
        distinct_count_estimate: 0,
    }
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
