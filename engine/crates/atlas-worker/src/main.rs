use anyhow::Result;
use clap::Parser;
use tonic::transport::Server;

mod ipc;
mod service;
mod split;
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = args.addr.parse()?;

    println!("atlas-worker listening on {addr}");
    Server::builder()
        .add_service(WorkerServiceServer::new(WorkerServiceImpl::default()))
        .serve(addr)
        .await?;
    Ok(())
}
