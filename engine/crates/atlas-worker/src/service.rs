//! `WorkerService` implementation: `Compile` parses SQL into the
//! partial/combine plan pair (`crate::split`), `ExecuteTask` runs one plan
//! (JSON-serialized `atlas_query::LogicalPlan`) against either a `.atlas`
//! file partition or an inline set of already-computed Arrow IPC batches —
//! the combine step handed back to whichever worker runs it — and
//! `Heartbeat` reports liveness plus in-flight task count for the
//! coordinator's worker registry.

use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::ipc;
use crate::split::split_for_distribution;
use crate::worker_pb::worker_service_server::WorkerService;
use crate::worker_pb::{
    task_request::Source, CompileRequest, CompileResponse, HeartbeatRequest, HeartbeatResponse,
    ResultBatch, TaskRequest,
};

#[derive(Default)]
pub struct WorkerServiceImpl {
    in_flight: AtomicI32,
}

struct CompiledQuery {
    logical_plan_json: String,
    optimized_plan_json: String,
    partial_plan_json: String,
    combine_plan_json: Option<String>,
}

fn compile_query(sql: &str, schema_json: &str) -> Result<CompiledQuery> {
    let schema: atlas_format::Schema =
        serde_json::from_str(schema_json).context("parsing dataset schema_json")?;
    let stmt = atlas_query::parse_sql(sql)?;
    let raw_plan = atlas_query::build_logical_plan(&stmt, &schema)?;
    let optimized_plan = atlas_optimizer::optimize(raw_plan.clone());
    let split = split_for_distribution(optimized_plan.clone());

    let logical_plan_json =
        serde_json::to_string(&raw_plan).context("serializing raw logical plan")?;
    let optimized_plan_json =
        serde_json::to_string(&optimized_plan).context("serializing optimized plan")?;
    let partial_plan_json =
        serde_json::to_string(&split.partial).context("serializing partial plan")?;
    let combine_plan_json = split
        .combine
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("serializing combine plan")?;
    Ok(CompiledQuery {
        logical_plan_json,
        optimized_plan_json,
        partial_plan_json,
        combine_plan_json,
    })
}

fn run_task(req: TaskRequest) -> Result<Vec<RecordBatch>> {
    let plan: atlas_query::LogicalPlan =
        serde_json::from_str(&req.plan_json).context("parsing task plan_json")?;

    let batches = match req.source.ok_or_else(|| anyhow!("task missing a source"))? {
        Source::File(f) => {
            let columns = if f.columns.is_empty() {
                None
            } else {
                Some(f.columns)
            };
            atlas_format::read_atlas_file(Path::new(&f.file_path), columns.as_deref())
                .with_context(|| format!("reading partition file {}", f.file_path))?
        }
        Source::Inline(inline) => {
            let mut batches = Vec::new();
            for blob in &inline.arrow_ipc_batches {
                batches.extend(ipc::decode_batches(blob)?);
            }
            batches
        }
    };

    atlas_exec::execute(&plan, batches)
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    type ExecuteTaskStream =
        Pin<Box<dyn Stream<Item = Result<ResultBatch, Status>> + Send + 'static>>;

    async fn compile(
        &self,
        request: Request<CompileRequest>,
    ) -> Result<Response<CompileResponse>, Status> {
        let req = request.into_inner();
        let response = match compile_query(&req.sql, &req.schema_json) {
            Ok(compiled) => CompileResponse {
                needs_combine: compiled.combine_plan_json.is_some(),
                partial_plan_json: compiled.partial_plan_json,
                combine_plan_json: compiled.combine_plan_json.unwrap_or_default(),
                logical_plan_json: compiled.logical_plan_json,
                optimized_plan_json: compiled.optimized_plan_json,
                error: String::new(),
            },
            Err(err) => CompileResponse {
                partial_plan_json: String::new(),
                combine_plan_json: String::new(),
                logical_plan_json: String::new(),
                optimized_plan_json: String::new(),
                needs_combine: false,
                error: format!("{err:#}"),
            },
        };
        Ok(Response::new(response))
    }

    async fn execute_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id.clone();

        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let result = run_task(req);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);

        let batches =
            result.map_err(|err| Status::internal(format!("task {task_id} failed: {err:#}")))?;
        let arrow_ipc = ipc::encode_batches(&batches)
            .map_err(|err| Status::internal(format!("task {task_id} encode failed: {err:#}")))?;

        let stream = tokio_stream::iter(vec![Ok(ResultBatch { arrow_ipc })]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse {
            alive: true,
            in_flight_tasks: self.in_flight.load(Ordering::SeqCst),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlas_format::{DataType, Field, Schema};

    fn schema_json() -> String {
        let schema = Schema::new(vec![
            Field::new("diagnosis", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("cost", DataType::Float64, false),
            Field::new("hospital", DataType::Utf8, false),
        ]);
        serde_json::to_string(&schema).unwrap()
    }

    /// Proves column pruning has a real physical effect end to end: the
    /// worker-compiled partial plan's `Scan.columns` is exactly the columns
    /// this query touches, not all 4 schema columns. Composes with Phase 2's
    /// `read_atlas_file` column-skipping test (which proves a restricted
    /// `columns` list physically skips bytes) to establish the full claim
    /// without re-implementing that byte-counting harness here.
    #[test]
    fn compile_prunes_partial_plan_scan_columns() {
        let compiled =
            compile_query("SELECT diagnosis FROM t WHERE age > 50", &schema_json()).unwrap();

        let partial: atlas_query::LogicalPlan =
            serde_json::from_str(&compiled.partial_plan_json).unwrap();
        let atlas_query::LogicalPlan::Project(project) = &partial else {
            panic!("expected Project at partial plan root, got {partial:?}");
        };
        let atlas_query::LogicalPlan::Filter(filter) = project.input.as_ref() else {
            panic!("expected Filter under Project, got {:?}", project.input);
        };
        let atlas_query::LogicalPlan::Scan(scan) = filter.input.as_ref() else {
            panic!("expected Scan under Filter, got {:?}", filter.input);
        };
        assert_eq!(
            scan.columns,
            vec!["age".to_string(), "diagnosis".to_string()]
        );
        assert!(!scan.columns.contains(&"cost".to_string()));
        assert!(!scan.columns.contains(&"hospital".to_string()));
    }
}
