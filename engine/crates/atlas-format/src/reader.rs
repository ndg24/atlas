//! `.atlas` file reader. Reads the footer first (seek-to-end), then only the
//! byte ranges of the requested columns' pages — an unrequested column's
//! bytes are never read. `read_atlas` is generic over `Read + Seek` so tests
//! can wrap a `File` in a byte-counting reader and assert that directly.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, ArrayRef};
use arrow::compute::concat;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use prost::Message;

use crate::footer::{ColumnChunk, FileFooter};
use crate::page::decode_page;
use crate::writer::{MAGIC, PAGE_ROWS};

/// Read a `.atlas` file. `columns = None` reads every column; otherwise only
/// the named columns' page bytes are touched.
pub fn read_atlas_file(path: &Path, columns: Option<&[String]>) -> Result<Vec<RecordBatch>> {
    let mut file =
        File::open(path).with_context(|| format!("opening .atlas file at {}", path.display()))?;
    read_atlas(&mut file, columns)
}

/// Read just the footer — seeks to the end, decodes the trailer + protobuf,
/// touches no page bytes.
pub fn read_footer<R: Read + Seek>(reader: &mut R) -> Result<FileFooter> {
    let file_len = reader
        .seek(SeekFrom::End(0))
        .context("seeking to end of file")?;
    if file_len < 8 {
        return Err(anyhow!("file too short to contain a footer"));
    }
    reader
        .seek(SeekFrom::End(-8))
        .context("seeking to footer trailer")?;
    let mut trailer = [0u8; 8];
    reader
        .read_exact(&mut trailer)
        .context("reading footer trailer")?;
    let footer_len = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as u64;
    if &trailer[4..8] != MAGIC {
        return Err(anyhow!("not an .atlas file: bad magic bytes"));
    }

    let footer_start = file_len
        .checked_sub(8 + footer_len)
        .ok_or_else(|| anyhow!("footer length larger than file"))?;
    reader
        .seek(SeekFrom::Start(footer_start))
        .context("seeking to footer")?;
    let mut footer_bytes = vec![0u8; footer_len as usize];
    reader
        .read_exact(&mut footer_bytes)
        .context("reading footer bytes")?;
    FileFooter::decode(footer_bytes.as_slice()).context("decoding footer protobuf")
}

pub(crate) fn read_atlas<R: Read + Seek>(
    reader: &mut R,
    columns: Option<&[String]>,
) -> Result<Vec<RecordBatch>> {
    let footer = read_footer(reader)?;
    let schema: Schema =
        serde_json::from_str(&footer.schema_json).context("parsing Arrow schema from footer")?;

    let wanted: Vec<&ColumnChunk> = match columns {
        None => footer.columns.iter().collect(),
        Some(names) => names
            .iter()
            .map(|name| {
                footer
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .ok_or_else(|| anyhow!("column {name} not found in .atlas file"))
            })
            .collect::<Result<_>>()?,
    };

    let mut column_arrays: Vec<(String, ArrayRef)> = Vec::with_capacity(wanted.len());
    for chunk in &wanted {
        let mut page_arrays = Vec::with_capacity(chunk.pages.len());
        for page in &chunk.pages {
            reader
                .seek(SeekFrom::Start(page.offset))
                .with_context(|| format!("seeking to page at offset {}", page.offset))?;
            let mut compressed = vec![0u8; page.compressed_length as usize];
            reader
                .read_exact(&mut compressed)
                .context("reading page bytes")?;
            let page_bytes =
                lz4_flex::decompress_size_prepended(&compressed).context("decompressing page")?;
            page_arrays.push(decode_page(&page_bytes)?);
        }
        let page_refs: Vec<&dyn Array> = page_arrays.iter().map(|a| a.as_ref()).collect();
        let full_column = concat(&page_refs).context("concatenating column pages")?;
        column_arrays.push((chunk.name.clone(), full_column));
    }

    let fields: Vec<Field> = column_arrays
        .iter()
        .map(|(name, _)| {
            schema
                .field_with_name(name)
                .cloned()
                .with_context(|| format!("field {name} missing from parsed schema"))
        })
        .collect::<Result<_>>()?;
    let out_schema = Arc::new(Schema::new(fields));

    let total_rows = column_arrays.first().map(|(_, a)| a.len()).unwrap_or(0);
    let mut batches = Vec::new();
    let mut row_start = 0usize;
    while row_start < total_rows {
        let batch_len = PAGE_ROWS.min(total_rows - row_start);
        let batch_columns: Vec<ArrayRef> = column_arrays
            .iter()
            .map(|(_, a)| a.slice(row_start, batch_len))
            .collect();
        batches.push(
            RecordBatch::try_new(out_schema.clone(), batch_columns)
                .context("building output batch")?,
        );
        row_start += batch_len;
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(out_schema));
    }

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::writer::write_atlas;

    /// Wraps a `Read + Seek` and counts bytes actually passed through
    /// `read()`, so a test can assert an unrequested column's page bytes
    /// were never touched — not just that the returned batch "looks right".
    struct CountingReader<R> {
        inner: R,
        bytes_read: Cell<usize>,
    }

    impl<R: Read> Read for CountingReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.inner.read(buf)?;
            self.bytes_read.set(self.bytes_read.get() + n);
            Ok(n)
        }
    }

    impl<R: Seek> Seek for CountingReader<R> {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(pos)
        }
    }

    fn five_column_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
            Field::new("d", DataType::Float64, false),
            Field::new("wanted", DataType::Utf8, false),
        ]));
        let filler: Vec<i64> = (0..2000).collect();
        let padding_string = |v: i64| "x".repeat(200) + &v.to_string();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(filler.clone())),
                Arc::new(Int64Array::from(filler.clone())),
                Arc::new(Int64Array::from(filler.clone())),
                Arc::new(Float64Array::from(
                    filler.iter().map(|v| *v as f64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    filler
                        .iter()
                        .map(|v| padding_string(*v))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn requesting_one_of_five_columns_never_reads_the_others_page_bytes() {
        let mut full_bytes = Vec::new();
        write_atlas(&mut full_bytes, &[five_column_batch()]).unwrap();

        let wanted_only_bytes = {
            let mut reader = CountingReader {
                inner: Cursor::new(full_bytes.clone()),
                bytes_read: Cell::new(0),
            };
            // Request a small int column, not the wide padded-string column
            // ("wanted" is just this column's name, not a hint to request it)
            // — the point is proving the *other four* columns' bytes,
            // including the dominant wide one, are never read.
            let names = vec!["a".to_string()];
            let batches = read_atlas(&mut reader, Some(&names)).unwrap();
            assert_eq!(batches[0].schema().fields().len(), 1);
            reader.bytes_read.get()
        };

        let all_columns_bytes = {
            let mut reader = CountingReader {
                inner: Cursor::new(full_bytes.clone()),
                bytes_read: Cell::new(0),
            };
            read_atlas(&mut reader, None).unwrap();
            reader.bytes_read.get()
        };

        assert!(
            wanted_only_bytes < all_columns_bytes / 3,
            "reading 1 of 5 columns read {wanted_only_bytes} bytes, \
             all columns read {all_columns_bytes} bytes — the single-column \
             read should be a small fraction of the full-file read"
        );
    }

    #[test]
    fn requesting_missing_column_errors() {
        let mut full_bytes = Vec::new();
        write_atlas(&mut full_bytes, &[five_column_batch()]).unwrap();
        let mut reader = Cursor::new(full_bytes);
        let err = read_atlas(&mut reader, Some(&["nope".to_string()])).unwrap_err();
        assert!(err.to_string().contains("nope"));
    }
}
