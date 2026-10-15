//! The model server in process: the same requests, faults and answers as over HTTP, without a
//! socket, for deterministic tests.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use crate::model::Delivered;
use crate::model::ModelServer;
use crate::transport::Attempt;
use crate::transport::Transport;
use crate::wire::Exchange;
use crate::wire::NoAnswer;
use crate::wire::Request;

/// One stream of a shared [`ModelServer`].
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "tests drive the model in process")
)]
#[derive(Debug, Clone)]
pub(crate) struct ModelTransport {
    server: Arc<Mutex<ModelServer>>,
    path: String,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "tests drive the model in process")
)]
impl ModelTransport {
    /// The transport of the stream at `path` on `server`.
    pub(crate) fn new(server: Arc<Mutex<ModelServer>>, path: impl Into<String>) -> Self {
        Self {
            server,
            path: path.into(),
        }
    }
}

impl Transport for ModelTransport {
    fn send(&self, request: &Request, _attempt: Attempt) -> Exchange {
        let delivered = self
            .server
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .exchange(&self.path, request);
        match delivered {
            Delivered::Answered { answer, .. } => {
                Exchange::Answered(Box::new(answer.parse(request)))
            }
            Delivered::Unreachable => Exchange::NoAnswer(NoAnswer::NotSent {
                reason: "unreachable (scripted)".to_owned(),
            }),
            Delivered::DroppedBeforeApply => Exchange::NoAnswer(NoAnswer::Lost {
                reason: "request dropped (scripted)".to_owned(),
            }),
            Delivered::DroppedAfterApply => Exchange::NoAnswer(NoAnswer::Lost {
                reason: "answer dropped (scripted)".to_owned(),
            }),
        }
    }
}
