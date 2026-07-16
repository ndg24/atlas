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

fn compile_query(sql: &str, schema_json: &str) -> Result<(String, Option<String>)> {
    let schema: atlas_format::Schema =
        serde_json::from_str(schema_json).context("parsing dataset schema_json")?;
    let stmt = atlas_query::parse_sql(sql)?;
    let plan = atlas_query::build_logical_plan(&stmt, &schema)?;
    let split = split_for_distribution(plan);

    let partial_json = serde_json::to_string(&split.partial).context("serializing partial plan")?;
    let combine_json = split
        .combine
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("serializing combine plan")?;
    Ok((partial_json, combine_json))
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
            Ok((partial_plan_json, combine_plan_json)) => CompileResponse {
                needs_combine: combine_plan_json.is_some(),
                partial_plan_json,
                combine_plan_json: combine_plan_json.unwrap_or_default(),
                error: String::new(),
            },
            Err(err) => CompileResponse {
                partial_plan_json: String::new(),
                combine_plan_json: String::new(),
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
