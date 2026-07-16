//! Wire format for one page: a single-column Arrow IPC stream. Reusing
//! Arrow's own IPC encoding (rather than a hand-rolled per-`DataType` binary
//! layout) is what lets every currently-supported and future Arrow type
//! round-trip through `.atlas` files for free — see the "Arrow as the type
//! system" design note in the README.

use std::io::Cursor;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

pub(crate) fn encode_page(field: &Field, array: ArrayRef) -> Result<Vec<u8>> {
    let schema = Schema::new(vec![field.clone()]);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![array])
        .context("building page batch")?;
    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, &schema).context("creating IPC stream writer")?;
        writer.write(&batch).context("writing IPC page batch")?;
        writer.finish().context("finishing IPC page stream")?;
    }
    Ok(buf)
}

pub(crate) fn decode_page(bytes: &[u8]) -> Result<ArrayRef> {
    let mut reader =
        StreamReader::try_new(Cursor::new(bytes), None).context("creating IPC stream reader")?;
    let batch = reader
        .next()
        .context("page stream is empty")?
        .context("reading IPC page batch")?;
    Ok(batch.column(0).clone())
}
