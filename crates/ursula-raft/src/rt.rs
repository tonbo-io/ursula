//! Runtime shim: switches project-owned `tokio` re-exports to `madsim-tokio`
//! under `cfg(madsim)` so virtual time/scheduling reaches every call site
//! that goes through it. Mirrors `ursula-runtime::rt`.

#[cfg(madsim)]
pub use sim_tokio::select;
#[cfg(madsim)]
pub use sim_tokio::spawn;
#[cfg(madsim)]
pub use sim_tokio::sync;
#[cfg(madsim)]
pub use sim_tokio::task;
#[cfg(madsim)]
pub use sim_tokio::time;
#[cfg(not(madsim))]
pub use tokio::select;
#[cfg(not(madsim))]
pub use tokio::spawn;
#[cfg(not(madsim))]
pub use tokio::sync;
#[cfg(not(madsim))]
pub use tokio::task;
#[cfg(not(madsim))]
pub use tokio::time;
