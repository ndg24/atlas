//! Hive-style partitioned writes: `dir/<col>=<value>/.../part-0.atlas`. This
//! convention is what makes Phase 5's Parquet/Iceberg interop a translation
//! exercise rather than a rewrite — both formats already lay files out the
//! same way.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use arrow::array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray, UInt32Array,
};
use arrow::compute::take_record_batch;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::footer::FileFooter;
use crate::writer::write_atlas_file;

pub type PartitionValues = Vec<(String, String)>;

/// Group `batches`' rows by the value(s) of `partition_by` columns and write
/// one `.atlas` file per distinct combination under `dir/<col>=<value>/...`.
pub fn write_partitioned(
    dir: &Path,
    batches: &[RecordBatch],
    partition_by: &[String],
) -> Result<Vec<(PartitionValues, PathBuf, FileFooter)>> {
    let mut groups: HashMap<PartitionValues, Vec<RecordBatch>> = HashMap::new();

    for batch in batches {
        let key_columns: Vec<&dyn Array> = partition_by
            .iter()
            .map(|name| {
                batch
                    .column_by_name(name)
                    .with_context(|| format!("partition column {name} not found in batch"))
                    .map(|c| c.as_ref())
            })
            .collect::<Result<_>>()?;

        let mut row_groups: HashMap<PartitionValues, Vec<u32>> = HashMap::new();
        for row in 0..batch.num_rows() {
            let key: PartitionValues = partition_by
                .iter()
                .zip(&key_columns)
                .map(|(name, col)| (name.clone(), value_as_string(*col, row)))
                .collect();
            row_groups.entry(key).or_default().push(row as u32);
        }

        for (key, indices) in row_groups {
            let indices = UInt32Array::from(indices);
            let taken =
                take_record_batch(batch, &indices).context("gathering rows for partition")?;
            groups.entry(key).or_default().push(taken);
        }
    }

    let mut written = Vec::with_capacity(groups.len());
    for (key, group_batches) in groups {
        let mut path = dir.to_path_buf();
        for (col, val) in &key {
            path.push(format!("{col}={val}"));
        }
        fs::create_dir_all(&path)
            .with_context(|| format!("creating partition directory {}", path.display()))?;
        path.push("part-0.atlas");

        let footer = write_atlas_file(&path, &group_batches)?;
        written.push((key, path, footer));
    }

    Ok(written)
}

fn value_as_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_string();
    }
    match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(row)
            .to_string(),
        other => panic!("unsupported partition column type: {other:?}"),
    }
}
