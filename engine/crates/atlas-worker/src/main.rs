use anyhow::Result;
use clap::Parser;
use tonic::transport::Server;

mod ipc;
mod obs_metrics;
mod service;
mod split;
mod telemetry;
mod worker_pb;

#[cfg(test)]
mod e2e_test;

use service::WorkerServiceImpl;
use worker_pb::worker_service_server::WorkerServiceServer;

#[derive(Parser)]
#[command(name = "atlas-worker")]
struct Args {
    /// Address this worker's gRPC server binds to.
    #[arg(long, default_value = "0.0.0.0:9100")]
    addr: String,

    /// Address the Prometheus /metrics HTTP listener binds to.
    #[arg(long, default_value = "0.0.0.0:9101")]
    metrics_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = args.addr.parse()?;
    let metrics_addr = args.metrics_addr.parse()?;

    telemetry::init_tracing();
    obs_metrics::init(metrics_addr)?;

    tracing::info!(%addr, %metrics_addr, "atlas-worker starting");
    Server::builder()
        .add_service(WorkerServiceServer::new(WorkerServiceImpl::default()))
        .serve(addr)
        .await?;

    telemetry::shutdown();
    Ok(())
}
