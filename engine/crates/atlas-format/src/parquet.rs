//! Native Parquet read/write. A thin wrapper over `parquet-rs`'s own Arrow
//! integration — the format logic (row groups, encoding, compression) is
//! entirely `parquet-rs`'s responsibility; this module only adapts its API to
//! the same shape `writer.rs`/`reader.rs` expose for `.atlas` files, so
//! `atlas-worker` can dispatch to either without caring which it's reading.

use std::fs::File;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
// Leading `::` forces resolution to the external `parquet` crate rather than
// this module (`crate::parquet`), which shares its name.
use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ::parquet::arrow::{ArrowWriter, ProjectionMask};

/// Write `batches` (all sharing one schema) to a single Parquet file at
/// `path`. Column statistics are not read back from Parquet's own row-group
/// metadata — callers that need catalog statistics should compute them from
/// the same in-memory `batches` via [`crate::compute_batches_column_stats`].
pub fn write_parquet(path: &Path, batches: &[RecordBatch]) -> Result<()> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .context("write_parquet requires at least one batch (for its schema)")?;
    let file = File::create(path)
        .with_context(|| format!("creating parquet file at {}", path.display()))?;
    let mut writer =
        ArrowWriter::try_new(file, schema, None).context("creating parquet arrow writer")?;
    for batch in batches {
        writer.write(batch).context("writing parquet batch")?;
    }
    writer.close().context("closing parquet writer")?;
    Ok(())
}

/// Read a Parquet file. `columns = None` reads every column; otherwise only
/// the named columns are materialized (via `parquet-rs`'s column projection,
/// which skips the other columns' pages at the row-group level).
pub fn read_parquet(path: &Path, columns: Option<&[String]>) -> Result<Vec<RecordBatch>> {
    let file =
        File::open(path).with_context(|| format!("opening parquet file at {}", path.display()))?;
    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("reading parquet metadata")?;

    if let Some(names) = columns {
        let parquet_schema = builder.parquet_schema();
        let indices: Vec<usize> = names
            .iter()
            .map(|name| {
                (0..parquet_schema.columns().len())
                    .find(|&i| parquet_schema.column(i).name() == name)
                    .ok_or_else(|| anyhow!("column {name} not found in parquet file"))
            })
            .collect::<Result<_>>()?;
        let mask = ProjectionMask::leaves(parquet_schema, indices);
        builder = builder.with_projection(mask);
    }

    let reader = builder.build().context("building parquet reader")?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("reading parquet record batches")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn requesting_missing_column_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing_col.parquet");
        write_parquet(&path, &[batch()]).unwrap();
        let err = read_parquet(&path, Some(&["nope".to_string()])).unwrap_err();
        assert!(err.to_string().contains("nope"));
    }
}
