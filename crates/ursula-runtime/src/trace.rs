//! In-process trace-context propagation across actor mailboxes.
//!
//! A [`tracing::Span`] does not follow a message across an mpsc channel: the
//! receiving actor runs in its own task scope. [`Traced<T>`] carries the
//! sender's current span next to the message so the receiver can re-establish
//! it as the parent of the work it performs, linking on-core execution back to
//! the originating request span.
//!
//! Capturing the current span is cheap (an `Arc` refcount bump) and free when
//! tracing is disabled, so this stays off the cost radar on the hot path.

use tracing::Span;

/// A message paired with the span that was current when it was enqueued.
pub(crate) struct Traced<T> {
    pub(crate) value: T,
    pub(crate) parent: Span,
}

impl<T> Traced<T> {
    /// Pair `value` with the caller's current span.
    pub(crate) fn capture(value: T) -> Self {
        Self {
            value,
            parent: Span::current(),
        }
    }
}
