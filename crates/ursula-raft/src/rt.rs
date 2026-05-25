//! Runtime shim: switches project-owned `tokio` re-exports to `madsim-tokio`
//! under `cfg(madsim)` so virtual time/scheduling reaches every call site
//! that goes through it. Mirrors `ursula-runtime::rt`.
#![allow(unused_imports)]

#[cfg(madsim)]
pub use sim_tokio::{spawn, sync, time};

#[cfg(not(madsim))]
pub use tokio::{spawn, sync, time};
