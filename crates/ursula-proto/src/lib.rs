//! Shared wire types.
//!
//! Module map:
//!
//! - [`durable`]: generated stream and storage protobuf messages.
//! - [`admin`]: shared administrative HTTP requests, responses and identity preconditions.
//! - [`telemetry`]: diagnostic snapshot types and the runtime metric field manifest.

pub mod admin;

pub mod durable {
    include!(concat!(env!("OUT_DIR"), "/ursula.durable.v1.rs"));
}

pub use durable::*;

/// Shared diagnostic snapshots and metric field manifest.
pub mod telemetry;
