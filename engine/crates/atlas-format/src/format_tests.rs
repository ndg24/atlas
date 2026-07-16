//! Round-trip, compression, and partitioning tests for the `.atlas` file
//! format — write_atlas_file/read_atlas_file/write_partitioned exercised
//! together, one level above the reader's own byte-range test.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};

use crate::{read_atlas_file, write_atlas_file, write_partitioned};

fn batch_with_all_types() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("d", DataType::Date32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])),
            Arc::new(StringArray::from(vec![Some("alice"), None, Some("carol")])),
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(Date32Array::from(vec![Some(19000), None, Some(19002)])),
        ],
    )
    .unwrap()
}

#[test]
fn round_trips_every_supported_type_including_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all_types.atlas");
    let original = batch_with_all_types();

    let footer = write_atlas_file(&path, &[original.clone()]).unwrap();
    assert_eq!(footer.row_count, 3);
    assert_eq!(footer.columns.len(), 5);

    let read_back = read_atlas_file(&path, None).unwrap();
    assert_eq!(read_back.len(), 1);
    assert_eq!(read_back[0], original);
}

#[test]
fn footer_statistics_match_source_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stats.atlas");
    let footer = write_atlas_file(&path, &[batch_with_all_types()]).unwrap();

    let int_col = footer.columns.iter().find(|c| c.name == "i").unwrap();
    let stats = int_col.stats.as_ref().unwrap();
    assert_eq!(i64::from_le_bytes(stats.min.clone().try_into().unwrap()), 1);
    assert_eq!(i64::from_le_bytes(stats.max.clone().try_into().unwrap()), 3);
    assert_eq!(stats.null_count, 1);

    let str_col = footer.columns.iter().find(|c| c.name == "s").unwrap();
    let str_stats = str_col.stats.as_ref().unwrap();
    assert_eq!(String::from_utf8(str_stats.min.clone()).unwrap(), "alice");
    assert_eq!(String::from_utf8(str_stats.max.clone()).unwrap(), "carol");
    assert_eq!(str_stats.null_count, 1);
}

#[test]
fn reading_subset_of_columns_returns_only_those_columns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("subset.atlas");
    write_atlas_file(&path, &[batch_with_all_types()]).unwrap();

    let names = vec!["s".to_string(), "b".to_string()];
    let batches = read_atlas_file(&path, Some(&names)).unwrap();
    assert_eq!(batches[0].schema().fields().len(), 2);
    assert_eq!(batches[0].schema().field(0).name(), "s");
    assert_eq!(batches[0].schema().field(1).name(), "b");
}

#[test]
fn lz4_compression_shrinks_a_repetitive_string_column() {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let repetitive = StringArray::from(vec!["same-value-over-and-over"; 5000]);
    let uncompressed_len: usize = repetitive
        .iter()
        .map(|v| v.map(str::len).unwrap_or(0))
        .sum();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(repetitive)]).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repetitive.atlas");
    write_atlas_file(&path, &[batch]).unwrap();
    let file_len = std::fs::metadata(&path).unwrap().len() as usize;

    assert!(
        file_len < uncompressed_len,
        "compressed file ({file_len} bytes) should be smaller than the raw string data ({uncompressed_len} bytes)"
    );
}

#[test]
fn partitioning_writes_one_file_per_distinct_value_and_each_reads_back_independently() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("hospital", DataType::Utf8, false),
        Field::new("patient_id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "c", "b", "a"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
        ],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let written = write_partitioned(dir.path(), &[batch], &["hospital".to_string()]).unwrap();
    assert_eq!(written.len(), 3);

    let mut total_rows = 0;
    for (key, path, footer) in &written {
        assert!(path.to_string_lossy().contains("hospital="));
        assert_eq!(key.len(), 1);
        assert_eq!(key[0].0, "hospital");
        total_rows += footer.row_count;

        let read_back = read_atlas_file(path, None).unwrap();
        let hospital_col = read_back[0]
            .column_by_name("hospital")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..hospital_col.len() {
            assert_eq!(hospital_col.value(i), key[0].1);
        }
    }
    assert_eq!(total_rows, 6);
}
