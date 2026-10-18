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

/// Spawns `future` on the runtime this thread runs on, or returns `None`
/// outside one: a value's drop may run where no runtime does.
#[cfg(not(madsim))]
pub(crate) fn try_spawn<F>(future: F) -> Option<task::JoinHandle<F::Output>>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::runtime::Handle::try_current()
        .ok()
        .map(|runtime| runtime.spawn(future))
}

/// Spawns `future` on the simulation's runtime, or returns `None` outside
/// one: a value's drop may run where no runtime does.
#[cfg(madsim)]
pub(crate) fn try_spawn<F>(future: F) -> Option<task::JoinHandle<F::Output>>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    madsim::runtime::Handle::try_current().ok()?;
    Some(sim_tokio::spawn(future))
}
