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

/// 128 random bits from the OS, or `None` when it has none to give.
#[cfg(not(madsim))]
pub(crate) fn random_u128() -> Option<u128> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).ok()?;
    Some(u128::from_le_bytes(bytes))
}

/// 128 random bits from the simulator's seeded generator (replayable).
#[cfg(madsim)]
#[expect(clippy::unnecessary_wraps, reason = "same signature as the OS version")]
pub(crate) fn random_u128() -> Option<u128> {
    Some(madsim::rand::random::<u128>())
}

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
