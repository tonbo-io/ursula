//! Shared wire types.
//!
//! Module map:
//!
//! - [`durable`]: generated stream and storage protobuf messages.
//! - [`admin`]: shared administrative HTTP requests, responses and identity preconditions.
//! - [`placement`]: the placement vocabulary of the meta group: node, log and placement ids,
//!   node lifecycles, replica roles, view stages and reconcile status.

pub mod admin;
pub mod placement;

pub mod durable {
    include!(concat!(env!("OUT_DIR"), "/ursula.durable.v1.rs"));
}

pub use durable::*;
