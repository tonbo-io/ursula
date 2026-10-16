//! Runtime shim: switches project-owned `tokio` re-exports to `madsim-tokio`
//! under `cfg(madsim)` so virtual time/scheduling reaches every call site
//! that goes through it. Mirrors `ursula-runtime::rt`.

#[cfg(madsim)]
pub use sim_tokio::select;
#[cfg(madsim)]
pub use sim_tokio::spawn;
/// tokio's `sync` under madsim, except `watch`: tokio's wakes the receivers
/// of one send in an order its process-seeded RNG picks, so the shim's
/// `watch` is [`crate::sim_watch`] there (#638).
#[cfg(madsim)]
pub mod sync {
    pub use sim_tokio::sync::Mutex;
    pub use sim_tokio::sync::Notify;
    pub use sim_tokio::sync::OwnedSemaphorePermit;
    pub use sim_tokio::sync::Semaphore;
    pub use sim_tokio::sync::mpsc;
    pub use sim_tokio::sync::oneshot;

    pub(crate) use crate::sim_watch as watch;
}
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
