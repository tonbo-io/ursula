//! Shared wire types.
//!
//! Module map:
//!
//! - [`telemetry`]: shared metric snapshots and WAL samples.
//! - [`durable`]: generated stream and storage protobuf messages.
//! - [`admin`]: shared administrative HTTP requests, responses and identity preconditions.

pub mod admin;
pub mod telemetry;

pub mod durable {
    include!(concat!(env!("OUT_DIR"), "/ursula.durable.v1.rs"));
}

pub use durable::*;
