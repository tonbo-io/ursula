//! Typed producer headers of Ursula's write keys (extensions Section 5).
//!
//! A producer write is keyed by `(Producer-Id, Producer-Epoch, Producer-Seq)`
//! within one `Stream-Incarnation`, with an optional body digest
//! (`Producer-Digest`) as part of its identity. Every answer to it is typed by
//! headers. The server renders its answers with this crate and clients parse
//! them with it, so the two sides cannot drift.
//!
//! The crate uses the standard library only, so a standalone client workspace
//! can depend on it by path. Headers come in as `(name, value)` pairs, with
//! names compared case-insensitively and values as raw bytes, which is what
//! every HTTP library's header map iterates as. They go out through a callback
//! that receives a lowercase name and a value.
//!
//! Module map:
//!
//! - [`names`]: the lowercase header names.
//! - [`value`]: field syntax: [`Digest`], producer integers, offsets and
//!   tokens.
//! - [`request`]: [`ProducerRequestHeaders`], the key, digest and token a
//!   write carries.
//! - [`answer`]: [`ProducerAnswer`], every typed answer to a producer write.
//! - [`error`]: the one parse error, [`HeaderError`].

pub mod answer;
pub mod error;
pub mod names;
pub mod request;
pub mod value;

mod fields;

pub use answer::ClosedBy;
pub use answer::ClosedEpoch;
pub use answer::Delivery;
pub use answer::Outcome;
pub use answer::ProducerAnswer;
pub use answer::Receipt;
pub use error::HeaderError;
pub use request::ProducerRequestHeaders;
pub use request::ProducerWrite;
pub use value::Digest;
pub use value::MAX_PRODUCER_INTEGER;
pub use value::PRODUCER_ID_MAX_BYTES;
