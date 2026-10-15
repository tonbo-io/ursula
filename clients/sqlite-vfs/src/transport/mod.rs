//! Transports: one attempt of a [`Request`], and what came of it ([`Exchange`]).
//!
//! A transport never retries, never follows a redirect and keeps no state about writes: the
//! replicator decides what to send again and when. The 0.8 driver (M1) calls it from blocking
//! workers; M3 swaps the runtime under it, not the callers.
//!
//! Module map:
//!
//! - [`http`]: [`http::HttpTransport`] over blocking HTTP/1.1, and the agents, TLS and failure
//!   classes the 0.7 client shares with it.
//! - `model`: `ModelTransport`, the model server in process (tests and the `model` feature).

pub(crate) mod http;
#[cfg(any(test, feature = "model"))]
pub(crate) mod model;

use crate::wire::Exchange;
use crate::wire::Request;

/// How an attempt goes out, after the attempts before it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Attempt {
    /// The attempt before got no answer: take a new connection rather than a pooled one that may
    /// be dead.
    pub(crate) fresh_connection: bool,
    /// The token was refused: read it again before sending.
    pub(crate) refresh_token: bool,
}

/// Sends one attempt of a request to one stream.
pub(crate) trait Transport: Send + Sync {
    fn send(&self, request: &Request, attempt: Attempt) -> Exchange;
}
