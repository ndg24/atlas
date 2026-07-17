//! End-to-end check of the actual wire path a distributed query takes:
//! `Compile` over gRPC -> `ExecuteTask` against two real `.atlas` partition
//! files -> `ExecuteTask` again with the combine plan over the two partials'
//! Arrow IPC bytes -> decode. Exercises AVG's sum/count decomposition
//! end-to-end (not just `split`'s unit tests), since that's the one path
//! that would silently produce a wrong-but-plausible number if the
//! partial/combine split were subtly off.

#![cfg(test)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use tonic::transport::Server;

use crate::service::WorkerServiceImpl;
use crate::worker_pb::task_request::Source;
use crate::worker_pb::worker_service_client::WorkerServiceClient;
use crate::worker_pb::worker_service_server::WorkerServiceServer;
use crate::worker_pb::{CompileRequest, FileSource, InlineSource, TaskRequest};

fn batch(diagnosis: &[&str], cost: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("diagnosis", DataType::Utf8, false),
        Field::new("cost", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(diagnosis.to_vec())),
            Arc::new(Float64Array::from(cost.to_vec())),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn distributed_group_by_with_avg_matches_hand_computed_baseline() {
    let addr: SocketAddr = "127.0.0.1:19199".parse().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerServiceServer::new(WorkerServiceImpl::default()))
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut client = WorkerServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connecting to in-process worker");

    let dir = tempfile::tempdir().unwrap();
    let part0 = dir.path().join("part0.atlas");
    let part1 = dir.path().join("part1.atlas");
    atlas_format::write_atlas_file(
        &part0,
        &[batch(&["flu", "flu", "cold"], &[100.0, 200.0, 50.0])],
    )
    .unwrap();
    atlas_format::write_atlas_file(&part1, &[batch(&["cold", "flu"], &[150.0, 75.0])]).unwrap();

    let schema = atlas_format::Schema::new(vec![
        Field::new("diagnosis", DataType::Utf8, false),
        Field::new("cost", DataType::Float64, false),
    ]);
    let schema_json = serde_json::to_string(&schema).unwrap();

    let compiled = client
        .compile(CompileRequest {
            sql: "SELECT diagnosis, COUNT(*) AS n, SUM(cost) AS total, AVG(cost) AS avg_cost \
                  FROM t GROUP BY diagnosis ORDER BY diagnosis"
                .to_string(),
            schema_json,
        })
        .await
        .expect("compile RPC")
        .into_inner();
    assert!(
        compiled.error.is_empty(),
        "compile error: {}",
        compiled.error
    );
    assert!(
        compiled.needs_combine,
        "GROUP BY + ORDER BY must need a combine step"
    );

    let mut partials = Vec::new();
    for (i, path) in [&part0, &part1].into_iter().enumerate() {
        let mut stream = client
            .execute_task(TaskRequest {
                task_id: format!("partial-{i}"),
                plan_json: compiled.partial_plan_json.clone(),
                source: Some(Source::File(FileSource {
                    file_path: path.to_string_lossy().into_owned(),
                    columns: vec![],
                    format: String::new(),
                })),
            })
            .await
            .unwrap_or_else(|e| panic!("execute_task partial-{i}: {e}"))
            .into_inner();
        let msg = stream
            .message()
            .await
            .unwrap_or_else(|e| panic!("receiving partial-{i}: {e}"))
            .expect("worker should return exactly one ResultBatch message");
        partials.push(msg.arrow_ipc);
    }

    let mut combine_stream = client
        .execute_task(TaskRequest {
            task_id: "combine".to_string(),
            plan_json: compiled.combine_plan_json.clone(),
            source: Some(Source::Inline(InlineSource {
                arrow_ipc_batches: partials,
            })),
        })
        .await
        .expect("execute_task combine")
        .into_inner();
    let final_msg = combine_stream
        .message()
        .await
        .expect("receiving combine result")
        .expect("combine worker should return exactly one ResultBatch message");
    let final_batches = crate::ipc::decode_batches(&final_msg.arrow_ipc).unwrap();

    // cold: cost {50, 150}      -> n=2 total=200 avg=100
    // flu:  cost {100, 200, 75} -> n=3 total=375 avg=125
    assert_eq!(final_batches.len(), 1);
    let batch = &final_batches[0];
    assert_eq!(batch.num_rows(), 2);

    let diagnosis = batch
        .column_by_name("diagnosis")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let n = batch
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let total = batch
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let avg = batch
        .column_by_name("avg_cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(diagnosis.value(0), "cold");
    assert_eq!(n.value(0), 2);
    assert_eq!(total.value(0), 200.0);
    assert_eq!(avg.value(0), 100.0);

    assert_eq!(diagnosis.value(1), "flu");
    assert_eq!(n.value(1), 3);
    assert_eq!(total.value(1), 375.0);
    assert_eq!(avg.value(1), 125.0);
}
