//! Injectable wall clock of the keyed engine and the in-memory object store
//! (design §6.1 U21).
//!
//! Wall-clock milliseconds stamp `published_at_ms`, space publications and
//! age objects for garbage collection and the orphan sweep. Production uses
//! [`SystemClock`]; the simulator injects a clock driven by its virtual
//! time, since `SystemTime` is not virtualized under `cfg(madsim)`.

use std::fmt;
#[cfg(not(madsim))]
use std::time::SystemTime;
#[cfg(not(madsim))]
use std::time::UNIX_EPOCH;

/// A source of wall-clock milliseconds since the Unix epoch.
pub trait Clock: Send + Sync {
    /// Milliseconds since the Unix epoch.
    fn now_ms(&self) -> u64;
}

impl fmt::Debug for dyn Clock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Clock").finish_non_exhaustive()
    }
}

/// The host's wall clock; the epoch under the simulator, which has no wall
/// clock (inject a virtual-time [`Clock`] there instead).
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

#[cfg(not(madsim))]
impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            })
    }
}

#[cfg(madsim)]
impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        0
    }
}
