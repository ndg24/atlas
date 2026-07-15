use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::{
    concat_batches, filter_record_batch, lexsort_to_indices, take, SortColumn, SortOptions,
};
use arrow::datatypes::{Field, Schema};

use atlas_query::{Expr, LogicalPlan, SortKey};

use crate::aggregate::exec_aggregate;
use crate::expr::eval_expr;

/// Phase 1 has exactly one data source (the loaded CSV) — scanning is the
/// identity operation. Phase 2+ makes this read from the catalog/`.atlas`
/// files instead.
pub fn exec_scan(source: Vec<RecordBatch>) -> Vec<RecordBatch> {
    source
}

pub fn exec_filter(batches: &[RecordBatch], predicate: &Expr) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            let mask = eval_expr(batch, predicate)?;
            let mask: &BooleanArray = mask
                .as_any()
                .downcast_ref()
                .ok_or_else(|| anyhow!("WHERE predicate did not evaluate to a boolean"))?;
            Ok(filter_record_batch(batch, mask)?)
        })
        .collect()
}

pub fn exec_project(
    batches: &[RecordBatch],
    exprs: &[Expr],
    aliases: &[String],
) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            let arrays: Vec<ArrayRef> = exprs
                .iter()
                .map(|e| eval_expr(batch, e))
                .collect::<Result<_>>()?;
            let fields: Vec<Field> = arrays
                .iter()
                .zip(aliases)
                .map(|(array, name)| Field::new(name, array.data_type().clone(), true))
                .collect();
            Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
        })
        .collect()
}

pub fn exec_sort(batches: &[RecordBatch], keys: &[SortKey]) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    let schema = batches[0].schema();
    let combined = concat_batches(&schema, batches)?;
    if combined.num_rows() == 0 || keys.is_empty() {
        return Ok(vec![combined]);
    }

    let sort_columns: Vec<SortColumn> = keys
        .iter()
        .map(|key| {
            Ok(SortColumn {
                values: eval_expr(&combined, &key.expr)?,
                options: Some(SortOptions {
                    descending: key.descending,
                    nulls_first: false,
                }),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let indices = lexsort_to_indices(&sort_columns, None)?;
    let columns: Vec<ArrayRef> = combined
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(vec![RecordBatch::try_new(combined.schema(), columns)?])
}

pub fn exec_limit(batches: &[RecordBatch], n: u64) -> Result<Vec<RecordBatch>> {
    let mut remaining = n as usize;
    let mut result = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take_rows = remaining.min(batch.num_rows());
        result.push(batch.slice(0, take_rows));
        remaining -= take_rows;
    }
    Ok(result)
}

/// Walk the logical plan bottom-up, dispatching each node to its executor.
/// No physical planning yet — the logical plan is executed directly.
pub fn execute(plan: &LogicalPlan, source: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    match plan {
        LogicalPlan::Scan(_) => Ok(exec_scan(source)),
        LogicalPlan::Filter(node) => {
            let input = execute(&node.input, source)?;
            exec_filter(&input, &node.predicate)
        }
        LogicalPlan::Project(node) => {
            let input = execute(&node.input, source)?;
            exec_project(&input, &node.exprs, &node.aliases)
        }
        LogicalPlan::Aggregate(node) => {
            let input = execute(&node.input, source)?;
            exec_aggregate(&input, &node.group_by, &node.aggregates)
        }
        LogicalPlan::Sort(node) => {
            let input = execute(&node.input, source)?;
            exec_sort(&input, &node.keys)
        }
        LogicalPlan::Limit(node) => {
            let input = execute(&node.input, source)?;
            exec_limit(&input, node.n)
        }
    }
}
