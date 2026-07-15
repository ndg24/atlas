use std::hash::{Hash, Hasher};

use anyhow::{bail, Result};
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;

/// A single scalar value extracted from an Arrow array at a given row,
/// used as a group-by key (needs `Eq`/`Hash`) or as an aggregate input.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
}

impl Eq for ScalarValue {}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ScalarValue::Null => 0u8.hash(state),
            ScalarValue::Int64(i) => {
                1u8.hash(state);
                i.hash(state);
            }
            ScalarValue::Float64(f) => {
                2u8.hash(state);
                f.to_bits().hash(state);
            }
            ScalarValue::Utf8(s) => {
                3u8.hash(state);
                s.hash(state);
            }
            ScalarValue::Boolean(b) => {
                4u8.hash(state);
                b.hash(state);
            }
        }
    }
}

pub fn scalar_at(array: &ArrayRef, row: usize) -> Result<ScalarValue> {
    if array.is_null(row) {
        return Ok(ScalarValue::Null);
    }
    Ok(match array.data_type() {
        DataType::Int64 => ScalarValue::Int64(
            array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        ),
        DataType::Float64 => ScalarValue::Float64(
            array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row),
        ),
        DataType::Utf8 => ScalarValue::Utf8(array.as_string::<i32>().value(row).to_string()),
        DataType::Boolean => ScalarValue::Boolean(array.as_boolean().value(row)),
        other => bail!("unsupported data type for group-by/aggregate value: {other:?}"),
    })
}
