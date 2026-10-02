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

/// Under the simulator: a process counter, reset at the start of every
/// simulated run ([`reset_random_for_sim`]), so a seed replays the same
/// object keys without drawing from the simulator's generator.
#[cfg(madsim)]
static SIM_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Unique within a simulated run (see [`SIM_NONCE`]).
#[cfg(madsim)]
#[expect(clippy::unnecessary_wraps, reason = "same signature as the OS version")]
pub(crate) fn random_u128() -> Option<u128> {
    let next = SIM_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Some(u128::from(next).wrapping_add(1))
}

/// Restarts the simulator's object-key nonces; call at the start of each
/// simulated run so that a seed replays the same keys.
#[cfg(madsim)]
pub fn reset_random_for_sim() {
    SIM_NONCE.store(0, std::sync::atomic::Ordering::Relaxed);
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
