//! Task and timer seam: the simulator's scheduler and virtual clock under
//! `cfg(madsim)`, tokio otherwise. The keyed engine spawns, sleeps, measures
//! deadlines and runs CPU-bound work only through this module, so the
//! indexer runs under deterministic simulation (design §6.1 U21).

#[cfg(madsim)]
pub(crate) use sim_tokio::spawn;
#[cfg(madsim)]
pub(crate) use sim_tokio::time;
#[cfg(not(madsim))]
pub(crate) use tokio::spawn;
#[cfg(not(madsim))]
pub(crate) use tokio::time;

/// Runs CPU-bound `work` off the async workers; inline under the simulator,
/// which has no blocking pool.
#[cfg(not(madsim))]
pub(crate) async fn run_blocking<F, R>(work: F) -> Result<R, String>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| error.to_string())
}

/// Runs CPU-bound `work` off the async workers; inline under the simulator,
/// which has no blocking pool.
#[cfg(madsim)]
pub(crate) async fn run_blocking<F, R>(work: F) -> Result<R, String>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    Ok(work())
}
