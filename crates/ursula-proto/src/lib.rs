//! Shared wire types.
//!
//! Module map:
//!
//! - [`durable`]: generated stream and storage protobuf messages.
//! - [`admin`]: administrative HTTP identity preconditions.

pub mod admin;

pub mod durable {
    include!(concat!(env!("OUT_DIR"), "/ursula.durable.v1.rs"));
}

pub use durable::*;
