//! `.atlas` file writer. Byte layout (see `docs/atlas-implementation-spec.md`
//! §1.2):
//!
//!   [Page 0][Page 1]...[Page N]
//!   [Footer]
//!   [Footer length: 4 bytes LE][Magic: "ATL1" 4 bytes]
//!
//! One contiguous run of pages per column, in schema order, so that reading
//! a subset of columns only ever touches those columns' byte ranges.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use arrow::array::{Array, ArrayRef};
use arrow::compute::concat;
use arrow::record_batch::RecordBatch;
use prost::Message;

use crate::footer::page_meta::Compression;
use crate::footer::{ColumnChunk, FileFooter, PageMeta};
use crate::page::encode_page;
use crate::stats::compute_statistics;

pub(crate) const PAGE_ROWS: usize = 8192;
pub(crate) const MAGIC: &[u8; 4] = b"ATL1";

/// Write `batches` (all sharing one schema) to a single `.atlas` file at
/// `path`, returning the footer that was written (statistics included).
pub fn write_atlas_file(path: &Path, batches: &[RecordBatch]) -> Result<FileFooter> {
    let file = File::create(path)
        .with_context(|| format!("creating .atlas file at {}", path.display()))?;
    write_atlas(file, batches)
}

pub(crate) fn write_atlas<W: Write>(mut writer: W, batches: &[RecordBatch]) -> Result<FileFooter> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .context("write_atlas_file requires at least one batch (for its schema)")?;
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    let mut offset: u64 = 0;
    let mut columns = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let parts: Vec<ArrayRef> = batches.iter().map(|b| b.column(col_idx).clone()).collect();
        let part_refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
        let column: ArrayRef = concat(&part_refs).context("concatenating column across batches")?;
        let stats = compute_statistics(column.as_ref());

        let mut pages = Vec::new();
        let mut row_start = 0usize;
        while row_start < column.len() {
            let page_len = PAGE_ROWS.min(column.len() - row_start);
            let page_array = column.slice(row_start, page_len);
            let page_bytes = encode_page(field, page_array)?;
            let compressed = lz4_flex::compress_prepend_size(&page_bytes);

            writer
                .write_all(&compressed)
                .context("writing page bytes")?;
            pages.push(PageMeta {
                offset,
                compressed_length: compressed.len() as u64,
                uncompressed_length: page_bytes.len() as u64,
                row_count: page_len as u32,
                compression: Compression::Lz4 as i32,
            });
            offset += compressed.len() as u64;
            row_start += page_len;
        }

        columns.push(ColumnChunk {
            name: field.name().clone(),
            pages,
            stats: Some(stats),
        });
    }

    let schema_json = serde_json::to_string(schema.as_ref()).context("serializing Arrow schema")?;
    let footer = FileFooter {
        columns,
        row_count,
        schema_json,
    };

    let footer_bytes = footer.encode_to_vec();
    writer.write_all(&footer_bytes).context("writing footer")?;
    writer
        .write_all(&(footer_bytes.len() as u32).to_le_bytes())
        .context("writing footer length")?;
    writer.write_all(MAGIC).context("writing magic bytes")?;

    Ok(footer)
}
