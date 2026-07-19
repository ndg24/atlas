//! Prometheus metrics for atlas-worker, exposed via a small built-in HTTP
//! listener (`metrics-exporter-prometheus`'s own server, not tonic — this
//! binary has no other HTTP surface). Named `obs_metrics` rather than
//! `metrics` so it doesn't shadow the `metrics` crate inside `main.rs`/
//! `service.rs`.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use metrics_exporter_prometheus::PrometheusBuilder;

/// Starts the Prometheus HTTP listener on `addr`. Call once, at startup.
pub fn init(addr: SocketAddr) -> Result<()> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .with_context(|| format!("installing Prometheus exporter on {addr}"))
}

/// Mirrors the worker's in-flight task count into a gauge.
pub fn set_in_flight(v: i32) {
    metrics::gauge!("atlas_worker_in_flight_tasks").set(v as f64);
}
