//! Arrow IPC encode/decode for the wire format `ResultBatch.arrow_ipc` and
//! `InlineSource.arrow_ipc_batches` use: each `Vec<u8>` is a complete,
//! self-contained IPC *stream* (schema + zero or more batches), not a bare
//! frame, so it round-trips independently of anything else on the wire.

use std::io::Cursor;

use anyhow::{Context, Result};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

pub fn encode_batches(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    if let Some(first) = batches.first() {
        let mut writer = StreamWriter::try_new(&mut buf, first.schema().as_ref())
            .context("creating Arrow IPC stream writer")?;
        for batch in batches {
            writer.write(batch).context("writing Arrow IPC batch")?;
        }
        writer.finish().context("finishing Arrow IPC stream")?;
    }
    Ok(buf)
}

pub fn decode_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader =
        StreamReader::try_new(Cursor::new(bytes), None).context("opening Arrow IPC stream")?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("reading Arrow IPC batches")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    #[test]
    fn round_trips_batches() {
        let batches = vec![batch(&[1, 2, 3]), batch(&[4, 5])];
        let bytes = encode_batches(&batches).unwrap();
        let decoded = decode_batches(&bytes).unwrap();
        let total: usize = decoded.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn empty_batches_round_trip_to_empty() {
        let bytes = encode_batches(&[]).unwrap();
        assert!(decode_batches(&bytes).unwrap().is_empty());
    }
}
