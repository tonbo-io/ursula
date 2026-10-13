//! Bounds on the request bodies the gateway holds in memory.
//!
//! The gateway buffers the body of each request it forwards whole, so that it
//! can send it again after a Raft leader redirect (see [`crate::Gateway`]).
//! [`UploadLimits::max_request_body_bytes`] bounds one body. [`Uploads`]
//! bounds them across requests, so that concurrent large or slow uploads
//! cannot exhaust the gateway's memory:
//!
//! - Before it reads any byte of a body, a request reserves the body's
//!   declared `Content-Length` from a node-wide budget
//!   ([`UploadLimits::budget_bytes`]). A body without a declared length
//!   reserves [`UploadLimits::small_body_max_bytes`] at first, and its
//!   reservation grows, from the part of the budget large bodies take, as the
//!   body arrives. The reservation shrinks to the body's size once it is
//!   buffered, and is released when the buffered body is dropped, after the
//!   gateway forwarded it.
//! - Part of the budget, the headroom, only bodies of at most
//!   [`UploadLimits::small_body_max_bytes`] may take, so that small commits
//!   never queue behind large uploads. Such a body takes the rest of the
//!   budget first and the headroom only when the rest is spent.
//! - A request that finds the budget spent waits for it for
//!   [`UploadLimits::budget_wait`] at most, then is refused (`503`).
//! - A body is cut, and refused (`408`) before anything of it is forwarded,
//!   only once its client has abandoned it: when no byte of it arrived for
//!   [`UploadLimits::stall_timeout`], or when fewer than
//!   [`UploadLimits::min_window_bytes`] arrived in one window of the stall
//!   timeout. Nothing bounds a reading in all, and neither rule looks at the
//!   body's size or at its average rate since the start, so a slow upload
//!   that keeps arriving is never cut.
//!
//! The per-tenant limits on large bodies are tenant admission limits
//! ([`crate::admission::TenantLimits::max_concurrent_uploads`] and
//! [`crate::admission::TenantLimits::max_upload_bytes`]), keyed by bucket
//! like the other tenant limits.
//!
//! The budget counts in units of [`UNIT_BYTES`] and rounds each reservation
//! up to a whole unit. [`UploadLimits`] can only be built valid
//! ([`UploadLimits::try_from`]): every body within the per-request limit fits
//! the part of the budget it may take, so no body waits for good.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::DEFAULT_MAX_REQUEST_BODY_BYTES;

/// The bytes one permit of the budget stands for. One acquisition takes at
/// most `u32::MAX` permits, so 1 KiB units fit a body of up to 4 TiB.
const UNIT_BYTES: u64 = 1024;

/// 256 MiB: half of a 512 MiB pod, which leaves the rest to the response
/// streams, connection buffers and the allocator's slack. Seven bodies at the
/// default 32 MiB limit fit beside the headroom.
pub const DEFAULT_BUDGET_BYTES: u64 = 256 << 20;
/// 32 MiB of the budget, room for at least 32 small bodies while large ones
/// hold the rest.
pub const DEFAULT_SMALL_BODY_HEADROOM_BYTES: u64 = 32 << 20;
/// 1 MiB: SQLite commits and appends are usually far smaller, and snapshots
/// usually larger.
pub const DEFAULT_SMALL_BODY_MAX_BYTES: u64 = 1 << 20;
/// One second: long enough for an upload that is about to end to release its
/// reservation, short enough that a client's retry, after the `503`'s
/// `Retry-After`, does not wait much longer than it would have here.
pub const DEFAULT_BUDGET_WAIT: Duration = Duration::from_secs(1);
/// 30 s without a byte of the body, and the length of the windows
/// [`DEFAULT_MIN_BYTES_PER_SEC`] is measured over. A client that is still
/// sending, however slowly, delivers some bytes every few seconds, so only a
/// body whose client went away without closing the connection (a vanished
/// host, a dropped NAT mapping) stalls this long. A client that gives up and
/// closes ends the reading at once. Three times the SQLite extension's 10 s
/// timeout for a commit, so the gateway never cuts a commit its client still
/// waits for.
pub const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(30);
/// 1 KiB/s, 30 KiB in each 30 s window: far below any link that still
/// carries data, so a live upload is never cut, while a connection that
/// trickles a byte now and then to hold its reservation is. The SQLite
/// extension gives a snapshot transfer 120 s in all: a snapshot this slow is
/// one it has given up on.
pub const DEFAULT_MIN_BYTES_PER_SEC: u64 = 1 << 10;

/// The values [`UploadLimits`] is built from, unchecked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadLimitsSpec {
    /// The largest body the gateway buffers for one request. Also the most
    /// it reads of a body before a refusal.
    pub max_request_body_bytes: usize,
    /// Bytes of request bodies the gateway holds at once, across requests,
    /// the headroom included.
    pub budget_bytes: u64,
    /// Bytes of the budget only small bodies may take.
    pub small_body_headroom_bytes: u64,
    /// The largest body, by its declared length, that counts as small. A
    /// body without a declared length counts as small until it grows past
    /// it.
    pub small_body_max_bytes: u64,
    /// How long a request waits for the budget before it is refused.
    pub budget_wait: Duration,
    /// How long a body may go without a byte arriving before it is cut, and
    /// the length of the windows `min_bytes_per_sec` is measured over.
    pub stall_timeout: Duration,
    /// The least rate at which a body must arrive, over each window of
    /// `stall_timeout` counted from the start of its reading. 0 for none.
    pub min_bytes_per_sec: u64,
}

impl Default for UploadLimitsSpec {
    fn default() -> Self {
        Self {
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            budget_bytes: DEFAULT_BUDGET_BYTES,
            small_body_headroom_bytes: DEFAULT_SMALL_BODY_HEADROOM_BYTES,
            small_body_max_bytes: DEFAULT_SMALL_BODY_MAX_BYTES,
            budget_wait: DEFAULT_BUDGET_WAIT,
            stall_timeout: DEFAULT_STALL_TIMEOUT,
            min_bytes_per_sec: DEFAULT_MIN_BYTES_PER_SEC,
        }
    }
}

/// Limits on the request bodies the gateway buffers (see the module docs),
/// checked: built only through [`UploadLimits::try_from`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadLimits {
    spec: UploadLimitsSpec,
}

impl Default for UploadLimits {
    /// The defaults, which are valid (`defaults_are_valid` checks it).
    fn default() -> Self {
        Self {
            spec: UploadLimitsSpec::default(),
        }
    }
}

impl TryFrom<UploadLimitsSpec> for UploadLimits {
    type Error = UploadLimitsError;

    fn try_from(spec: UploadLimitsSpec) -> Result<Self, Self::Error> {
        if spec.max_request_body_bytes == 0 {
            return Err(UploadLimitsError::ZeroRequestBodyLimit);
        }
        let shared_units =
            units(spec.budget_bytes).saturating_sub(units(spec.small_body_headroom_bytes));
        let limit_units = units(u64::try_from(spec.max_request_body_bytes).unwrap_or(u64::MAX));
        if limit_units > shared_units {
            return Err(UploadLimitsError::LimitAboveSharedBudget {
                max_request_body_bytes: spec.max_request_body_bytes,
                shared_bytes: u64::from(shared_units).saturating_mul(UNIT_BYTES),
            });
        }
        if units(spec.small_body_max_bytes) > units(spec.small_body_headroom_bytes) {
            return Err(UploadLimitsError::SmallBodyAboveHeadroom {
                small_body_max_bytes: spec.small_body_max_bytes,
                headroom_bytes: spec.small_body_headroom_bytes,
            });
        }
        if spec.stall_timeout.is_zero() {
            return Err(UploadLimitsError::ZeroStallTimeout);
        }
        Ok(Self { spec })
    }
}

impl UploadLimits {
    /// The values the limits were built from.
    pub fn spec(&self) -> &UploadLimitsSpec {
        &self.spec
    }

    pub fn max_request_body_bytes(&self) -> usize {
        self.spec.max_request_body_bytes
    }

    pub fn budget_bytes(&self) -> u64 {
        self.spec.budget_bytes
    }

    pub fn small_body_headroom_bytes(&self) -> u64 {
        self.spec.small_body_headroom_bytes
    }

    pub fn small_body_max_bytes(&self) -> u64 {
        self.spec.small_body_max_bytes
    }

    pub fn budget_wait(&self) -> Duration {
        self.spec.budget_wait
    }

    pub fn stall_timeout(&self) -> Duration {
        self.spec.stall_timeout
    }

    /// The bytes a body must deliver in each window of
    /// [`UploadLimits::stall_timeout`]: [`UploadLimitsSpec::min_bytes_per_sec`]
    /// over the window.
    pub fn min_window_bytes(&self) -> u64 {
        let millis = u64::try_from(self.spec.stall_timeout.as_millis()).unwrap_or(u64::MAX);
        let bytes = u128::from(self.spec.min_bytes_per_sec)
            .saturating_mul(u128::from(millis))
            .checked_div(1000)
            .unwrap_or(0);
        u64::try_from(bytes).unwrap_or(u64::MAX)
    }
}

/// Why an [`UploadLimitsSpec`] is not valid.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum UploadLimitsError {
    #[error("the request body limit must be at least 1 byte")]
    ZeroRequestBodyLimit,
    #[error(
        "the request body limit of {max_request_body_bytes} bytes is over the {shared_bytes} \
         bytes of the body budget that large bodies may take (the budget less the small-body \
         headroom): raise the budget with the limit"
    )]
    LimitAboveSharedBudget {
        max_request_body_bytes: usize,
        shared_bytes: u64,
    },
    #[error(
        "small bodies of up to {small_body_max_bytes} bytes do not fit the small-body headroom of \
         {headroom_bytes} bytes"
    )]
    SmallBodyAboveHeadroom {
        small_body_max_bytes: u64,
        headroom_bytes: u64,
    },
    #[error("the request body stall timeout must not be zero")]
    ZeroStallTimeout,
}

/// The budget stayed spent for [`UploadLimits::budget_wait`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BudgetExhausted;

/// One part of the budget, as a semaphore of [`UNIT_BYTES`] permits.
struct Pool {
    semaphore: Arc<Semaphore>,
    units: u32,
}

impl Pool {
    fn new(units: u32) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(units as usize)),
            units,
        }
    }

    /// Units reserved from this pool now.
    fn reserved(&self) -> u64 {
        u64::from(self.units)
            .saturating_sub(u64::try_from(self.semaphore.available_permits()).unwrap_or(u64::MAX))
    }
}

/// The gateway's budget for request bodies.
pub(crate) struct Uploads {
    small_body_max_bytes: u64,
    budget_wait: Duration,
    /// The budget less the headroom: any body may take it.
    shared: Pool,
    /// Only small bodies may take it.
    headroom: Pool,
}

impl Uploads {
    pub(crate) fn new(limits: &UploadLimits) -> Self {
        let budget = units(limits.budget_bytes());
        let headroom = units(limits.small_body_headroom_bytes());
        Self {
            small_body_max_bytes: limits.small_body_max_bytes(),
            budget_wait: limits.budget_wait(),
            shared: Pool::new(budget.saturating_sub(headroom)),
            headroom: Pool::new(headroom),
        }
    }

    /// Whether a body of `bytes` counts as small: it may take the headroom,
    /// and it counts toward no tenant's upload limits.
    pub(crate) fn is_small(&self, bytes: u64) -> bool {
        bytes <= self.small_body_max_bytes
    }

    /// The first reservation of a body without a declared length.
    pub(crate) fn undeclared_start(&self) -> u64 {
        self.small_body_max_bytes
    }

    /// Reserves `bytes` for a body, waiting for it for
    /// [`UploadLimits::budget_wait`] at most. A small body takes the shared
    /// part of the budget first and the headroom when that is spent.
    pub(crate) async fn reserve(&self, bytes: u64) -> Result<BodyPermit, BudgetExhausted> {
        let units = units(bytes);
        let shared = (Part::Shared, &self.shared.semaphore);
        let headroom = (Part::Headroom, &self.headroom.semaphore);
        let asks: &[(Part, &Arc<Semaphore>)] = if self.is_small(bytes) {
            &[shared, headroom]
        } else {
            &[shared]
        };
        let (part, permit) = acquire(asks, units, self.budget_wait)
            .await
            .ok_or(BudgetExhausted)?;
        Ok(BodyPermit {
            permit,
            part,
            bytes,
        })
    }

    /// Grows `permit` to `bytes`, from the shared part of the budget, waiting
    /// for it for [`UploadLimits::budget_wait`] at most. A permit that held
    /// headroom gives it back once it has the whole of `bytes` from the shared
    /// part: a body that outgrew the small ones leaves the headroom to them.
    pub(crate) async fn grow(
        &self,
        permit: &mut BodyPermit,
        bytes: u64,
    ) -> Result<(), BudgetExhausted> {
        if bytes <= permit.bytes {
            return Ok(());
        }
        let wanted = units(bytes);
        let more = match permit.part {
            Part::Shared => wanted
                .saturating_sub(u32::try_from(permit.permit.num_permits()).unwrap_or(u32::MAX)),
            Part::Headroom => wanted,
        };
        let shared = [(Part::Shared, &self.shared.semaphore)];
        let (_, added) = acquire(&shared, more, self.budget_wait)
            .await
            .ok_or(BudgetExhausted)?;
        match permit.part {
            Part::Shared => permit.permit.merge(added),
            // The headroom's permits go back as the old permit drops.
            Part::Headroom => {
                permit.permit = added;
                permit.part = Part::Shared;
            }
        }
        permit.bytes = bytes;
        Ok(())
    }

    /// Bytes reserved now, in whole units.
    pub(crate) fn reserved_bytes(&self) -> u64 {
        self.shared
            .reserved()
            .saturating_add(self.headroom.reserved())
            .saturating_mul(UNIT_BYTES)
    }
}

/// Which part of the budget a permit holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Part {
    Shared,
    Headroom,
}

/// Takes `units` permits from the first semaphore of `asks` that has them
/// now, else from the first that gets them within `wait`. A losing
/// acquisition gives back what it was assigned when it drops, and so does
/// one whose request is dropped while it waits.
async fn acquire(
    asks: &[(Part, &Arc<Semaphore>)],
    units: u32,
    wait: Duration,
) -> Option<(Part, OwnedSemaphorePermit)> {
    for (part, semaphore) in asks {
        if let Ok(permit) = Arc::clone(semaphore).try_acquire_many_owned(units) {
            return Some((*part, permit));
        }
    }
    let acquiring = |(part, semaphore): &(Part, &Arc<Semaphore>)| {
        let semaphore = Arc::clone(semaphore);
        let part = *part;
        async move { (part, semaphore.acquire_many_owned(units).await) }
    };
    let waiting = async {
        match asks {
            [first, second] => tokio::select! {
                acquired = acquiring(first) => Some(acquired),
                acquired = acquiring(second) => Some(acquired),
            },
            [only] => Some(acquiring(only).await),
            _ => None,
        }
    };
    let (part, permit) = tokio::time::timeout(wait, waiting).await.ok()??;
    // The semaphores are never closed.
    permit.ok().map(|permit| (part, permit))
}

/// A body's share of the budget, released when it drops. The gateway holds
/// it for as long as it holds the body.
pub(crate) struct BodyPermit {
    permit: OwnedSemaphorePermit,
    part: Part,
    bytes: u64,
}

impl BodyPermit {
    /// The bytes the permit covers.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Shrinks the permit to `bytes`, the size of the body as buffered, when
    /// that is less than it covers.
    pub(crate) fn settle(&mut self, bytes: u64) {
        if bytes >= self.bytes {
            return;
        }
        self.bytes = bytes;
        let excess = self
            .permit
            .num_permits()
            .saturating_sub(units(bytes) as usize);
        drop(self.permit.split(excess));
    }
}

/// `bytes` in whole [`UNIT_BYTES`], rounded up.
fn units(bytes: u64) -> u32 {
    u32::try_from(bytes.div_ceil(UNIT_BYTES)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::UploadLimits;
    use super::UploadLimitsError;
    use super::UploadLimitsSpec;
    use super::Uploads;

    fn limits(spec: UploadLimitsSpec) -> Result<UploadLimits, UploadLimitsError> {
        UploadLimits::try_from(spec)
    }

    #[test]
    fn defaults_are_valid() {
        assert_eq!(
            limits(UploadLimitsSpec::default()),
            Ok(UploadLimits::default())
        );
    }

    /// Each limit that would leave a body waiting for good, or cut every
    /// body at once, is refused with its own error.
    #[test]
    fn invalid_limits_are_refused() {
        let spec = UploadLimitsSpec::default;
        assert_eq!(
            limits(UploadLimitsSpec {
                max_request_body_bytes: 0,
                ..spec()
            }),
            Err(UploadLimitsError::ZeroRequestBodyLimit)
        );
        // A headroom of the whole budget leaves nothing to a large body.
        assert_eq!(
            limits(UploadLimitsSpec {
                budget_bytes: 32 << 20,
                ..spec()
            }),
            Err(UploadLimitsError::LimitAboveSharedBudget {
                max_request_body_bytes: 32 << 20,
                shared_bytes: 0,
            })
        );
        assert_eq!(
            limits(UploadLimitsSpec {
                max_request_body_bytes: 1 << 30,
                ..spec()
            }),
            Err(UploadLimitsError::LimitAboveSharedBudget {
                max_request_body_bytes: 1 << 30,
                shared_bytes: 224 << 20,
            })
        );
        assert_eq!(
            limits(UploadLimitsSpec {
                small_body_headroom_bytes: 512 << 10,
                ..spec()
            }),
            Err(UploadLimitsError::SmallBodyAboveHeadroom {
                small_body_max_bytes: 1 << 20,
                headroom_bytes: 512 << 10,
            })
        );
        assert_eq!(
            limits(UploadLimitsSpec {
                stall_timeout: Duration::ZERO,
                ..spec()
            }),
            Err(UploadLimitsError::ZeroStallTimeout)
        );
        // The limit may take the whole shared part.
        limits(UploadLimitsSpec {
            max_request_body_bytes: 224 << 20,
            ..spec()
        })
        .expect("the limit may take the whole shared part");
    }

    /// 4 KiB of shared budget and 2 KiB of headroom for bodies of up to
    /// 1 KiB, waiting 20 ms.
    fn tiny() -> Uploads {
        Uploads::new(
            &limits(UploadLimitsSpec {
                max_request_body_bytes: 4 << 10,
                budget_bytes: 6 << 10,
                small_body_headroom_bytes: 2 << 10,
                small_body_max_bytes: 1 << 10,
                budget_wait: Duration::from_millis(20),
                ..UploadLimitsSpec::default()
            })
            .unwrap(),
        )
    }

    /// A permit that grows past the small bodies moves to the shared part and
    /// gives the headroom back, and one that cannot grow within the wait is
    /// refused and keeps what it had.
    #[tokio::test]
    async fn a_growing_permit_leaves_the_headroom() {
        let uploads = tiny();
        let large = uploads.reserve(4 << 10).await.unwrap();
        let mut small = uploads.reserve(1 << 10).await.unwrap();
        assert_eq!(uploads.headroom.reserved(), 1);

        assert_eq!(
            uploads.grow(&mut small, 2 << 10).await,
            Err(super::BudgetExhausted)
        );
        assert_eq!(small.bytes(), 1 << 10);
        drop(large);
        uploads.grow(&mut small, 2 << 10).await.unwrap();
        assert_eq!(
            (uploads.headroom.reserved(), uploads.shared.reserved()),
            (0, 2)
        );
        uploads.grow(&mut small, 3 << 10).await.unwrap();
        assert_eq!(uploads.shared.reserved(), 3);
        small.settle(10);
        assert_eq!(uploads.reserved_bytes(), 1 << 10);
        drop(small);
        assert_eq!(uploads.reserved_bytes(), 0);
    }

    /// A request dropped while it waits for the budget gives back whatever
    /// it was assigned.
    #[tokio::test]
    async fn a_request_dropped_while_it_waits_takes_nothing() {
        let uploads = tiny();
        let held = uploads.reserve(3 << 10).await.unwrap();
        let waiting = uploads.reserve(4 << 10);
        let mut waiting = Box::pin(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), &mut waiting)
                .await
                .is_err()
        );
        drop(waiting);
        drop(held);
        assert_eq!(uploads.reserved_bytes(), 0);
        uploads
            .reserve(4 << 10)
            .await
            .expect("the whole shared part is free again");
    }
}
