//! Shared wire types.
//!
//! Module map:
//!
//! - [`durable`]: generated stream and storage protobuf messages.
//! - [`admin`]: shared administrative HTTP requests, responses and identity preconditions.
//! - [`telemetry`]: storage measurement values shared by runtime and Raft.

pub mod admin;

pub mod durable {
    include!(concat!(env!("OUT_DIR"), "/ursula.durable.v1.rs"));
}

pub use durable::*;

/// Shared storage telemetry values.
pub mod telemetry;
