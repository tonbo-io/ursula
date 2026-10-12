//! Bounds on the request bodies the gateway holds in memory.
//!
//! The gateway buffers the body of each request it forwards whole, so that it
//! can send it again after a Raft leader redirect (see [`crate::Gateway`]).
//! `max_request_body_bytes` bounds one body. [`Uploads`] bounds them across
//! requests, so that concurrent large or slow uploads cannot exhaust the
//! gateway's memory, and one tenant cannot take the memory of every other:
//!
//! - Before it reads any byte of a body, a request reserves the body's
//!   declared `Content-Length`, or the per-request limit when the length is
//!   unknown, from a node-wide budget ([`UploadLimits::budget_bytes`]). The
//!   reservation shrinks to the body's size once it is buffered, and is
//!   released when the buffered body is dropped, after the gateway forwarded
//!   it.
//! - Part of the budget, the headroom, only bodies of at most
//!   [`UploadLimits::small_body_max_bytes`] may take, so that small commits
//!   never queue behind large uploads. Such a body takes the rest of the
//!   budget first and the headroom only when the rest is spent.
//! - A request that finds the budget spent waits for it for
//!   [`UploadLimits::budget_wait`] at most, then is refused (`503`).
//! - Each tenant, a bucket and the principal that authenticated the request,
//!   holds [`UploadLimits::tenant_uploads`] reservations and
//!   [`UploadLimits::tenant_bytes`] bytes at most. A request past either is
//!   refused at once (`429`). A tenant's first reservation always passes its
//!   byte cap, so a cap below the per-request limit cannot refuse a body for
//!   good.
//!
//! The budget counts in units of [`UNIT_BYTES`] and rounds each reservation
//! up to a whole unit. A reservation larger than the part of the budget it may
//! take is cut to that part, so that no body is refused for good: it then
//! waits until it has the part to itself. Tenants are tracked only while they
//! hold a reservation, so the table holds no more entries than the uploads
//! under way.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::usage::PrincipalRef;

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
/// 64 MiB, a quarter of the default budget: two bodies at the default 32 MiB
/// limit, so that four tenants uploading at once cannot spend the budget.
pub const DEFAULT_TENANT_BYTES: u64 = 64 << 20;
/// Eight bodies at once per tenant: a SQLite database sends one commit at a
/// time, and a snapshot beside it.
pub const DEFAULT_TENANT_UPLOADS: u32 = 8;
/// One second: long enough for an upload that is about to end to release its
/// reservation, short enough that a client's retry, after the `503`'s
/// `Retry-After`, does not wait much longer than it would have here.
pub const DEFAULT_BUDGET_WAIT: Duration = Duration::from_secs(1);
/// 15 s: past the SQLite extension's 10 s request timeout for a commit, plus
/// margin, so the gateway never cuts a commit its client still waits for.
pub const DEFAULT_READ_GRACE: Duration = Duration::from_secs(15);
/// 256 KiB/s: a 32 MiB body may take 128 s, past the SQLite extension's
/// 120 s timeout for snapshot transfers, so the gateway cuts only bodies whose
/// client gave up.
pub const DEFAULT_MIN_BYTES_PER_SEC: u64 = 256 << 10;

/// Limits on the request bodies the gateway buffers (see the module docs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadLimits {
    /// Bytes of request bodies the gateway holds at once, across requests,
    /// the headroom included.
    pub budget_bytes: NonZeroU64,
    /// Bytes of the budget only small bodies may take. Must be below
    /// `budget_bytes` ([`UploadLimits::check`]).
    pub small_body_headroom_bytes: u64,
    /// The largest body, by its declared length, that counts as small. A body
    /// without a declared length counts as large.
    pub small_body_max_bytes: u64,
    /// Bytes one tenant may hold at once. Its first body always passes.
    pub tenant_bytes: NonZeroU64,
    /// Bodies one tenant may have under way at once.
    pub tenant_uploads: NonZeroU32,
    /// How long a request waits for the budget before it is refused.
    pub budget_wait: Duration,
    /// How long a body may take to arrive whatever its pace.
    pub read_grace: Duration,
    /// After `read_grace`, the least average rate since the reading started.
    /// A body that falls below it is cut and refused (`408`).
    pub min_bytes_per_sec: NonZeroU64,
}

impl Default for UploadLimits {
    fn default() -> Self {
        Self {
            budget_bytes: NonZeroU64::new(DEFAULT_BUDGET_BYTES).unwrap_or(NonZeroU64::MIN),
            small_body_headroom_bytes: DEFAULT_SMALL_BODY_HEADROOM_BYTES,
            small_body_max_bytes: DEFAULT_SMALL_BODY_MAX_BYTES,
            tenant_bytes: NonZeroU64::new(DEFAULT_TENANT_BYTES).unwrap_or(NonZeroU64::MIN),
            tenant_uploads: NonZeroU32::new(DEFAULT_TENANT_UPLOADS).unwrap_or(NonZeroU32::MIN),
            budget_wait: DEFAULT_BUDGET_WAIT,
            read_grace: DEFAULT_READ_GRACE,
            min_bytes_per_sec: NonZeroU64::new(DEFAULT_MIN_BYTES_PER_SEC)
                .unwrap_or(NonZeroU64::MIN),
        }
    }
}

impl UploadLimits {
    /// Refuses limits that would leave no budget for large bodies: a headroom
    /// of the whole budget, or of all but less than one unit of it.
    pub fn check(&self) -> Result<(), UploadLimitsError> {
        let budget = units(self.budget_bytes.get());
        if units(self.small_body_headroom_bytes) >= budget {
            return Err(UploadLimitsError::HeadroomNotBelowBudget {
                headroom_bytes: self.small_body_headroom_bytes,
                budget_bytes: self.budget_bytes,
            });
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UploadLimitsError {
    #[error(
        "the small-body headroom of {headroom_bytes} bytes leaves nothing of the body budget of \
         {budget_bytes} bytes for large bodies"
    )]
    HeadroomNotBelowBudget {
        headroom_bytes: u64,
        budget_bytes: NonZeroU64,
    },
}

/// Whose reservations count together against [`UploadLimits::tenant_bytes`]
/// and [`UploadLimits::tenant_uploads`]: the request's bucket, and the
/// principal that authenticated it when access control is installed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct UploadTenant {
    pub(crate) bucket_id: String,
    pub(crate) principal: Option<PrincipalRef>,
}

/// Why a request got no reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UploadRefusal {
    /// The tenant holds [`UploadLimits::tenant_bytes`], with this body.
    TenantBytes,
    /// The tenant has [`UploadLimits::tenant_uploads`] bodies under way.
    TenantUploads,
    /// The budget stayed spent for [`UploadLimits::budget_wait`].
    BudgetExhausted,
}

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

    /// The permits a reservation of `units` takes from this pool: all of
    /// them, or the whole pool when it is smaller. `None` for an empty pool.
    fn ask(&self, units: u32) -> Option<(Arc<Semaphore>, u32)> {
        (self.units > 0).then(|| (Arc::clone(&self.semaphore), units.min(self.units)))
    }

    /// Units reserved from this pool now.
    fn reserved(&self) -> u64 {
        u64::from(self.units)
            .saturating_sub(u64::try_from(self.semaphore.available_permits()).unwrap_or(u64::MAX))
    }
}

/// What one tenant holds now.
#[derive(Debug, Default)]
struct TenantUse {
    bytes: u64,
    uploads: u32,
}

/// The gateway's budget for request bodies and its per-tenant caps.
pub(crate) struct Uploads {
    limits: UploadLimits,
    /// The budget less the headroom: any body may take it.
    shared: Pool,
    /// Only small bodies may take it.
    headroom: Pool,
    tenants: Mutex<HashMap<UploadTenant, TenantUse>>,
}

impl Uploads {
    pub(crate) fn new(limits: UploadLimits) -> Self {
        let budget = units(limits.budget_bytes.get());
        let headroom = units(limits.small_body_headroom_bytes).min(budget);
        Self {
            shared: Pool::new(budget.saturating_sub(headroom)),
            headroom: Pool::new(headroom),
            tenants: Mutex::new(HashMap::new()),
            limits,
        }
    }

    pub(crate) fn limits(&self) -> &UploadLimits {
        &self.limits
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<UploadTenant, TenantUse>> {
        self.tenants.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reserves `bytes` for a body of `tenant`: first against the tenant's
    /// caps, at once, then from the budget, waiting for it for
    /// [`UploadLimits::budget_wait`] at most.
    pub(crate) async fn reserve(
        self: &Arc<Self>,
        tenant: UploadTenant,
        bytes: u64,
    ) -> Result<Reservation, UploadRefusal> {
        let claim = self.claim(tenant, bytes)?;
        let units = units(bytes);
        let asks = if bytes <= self.limits.small_body_max_bytes {
            [self.shared.ask(units), self.headroom.ask(units)]
        } else {
            [self.shared.ask(units), None]
        };
        let permit = acquire(asks, self.limits.budget_wait)
            .await
            .ok_or(UploadRefusal::BudgetExhausted)?;
        Ok(Reservation { claim, permit })
    }

    /// Counts a body of `bytes` against `tenant`'s caps.
    fn claim(self: &Arc<Self>, tenant: UploadTenant, bytes: u64) -> Result<Claim, UploadRefusal> {
        let mut tenants = self.lock();
        let held = tenants.entry(tenant.clone()).or_default();
        // Neither refusal can leave a new, empty entry behind: both need an
        // upload under way.
        if held.uploads >= self.limits.tenant_uploads.get() {
            return Err(UploadRefusal::TenantUploads);
        }
        if held.uploads > 0 && held.bytes.saturating_add(bytes) > self.limits.tenant_bytes.get() {
            return Err(UploadRefusal::TenantBytes);
        }
        held.uploads = held.uploads.saturating_add(1);
        held.bytes = held.bytes.saturating_add(bytes);
        Ok(Claim {
            uploads: Arc::clone(self),
            tenant,
            bytes,
        })
    }

    /// Bytes reserved now, in whole units.
    pub(crate) fn reserved_bytes(&self) -> u64 {
        self.shared
            .reserved()
            .saturating_add(self.headroom.reserved())
            .saturating_mul(UNIT_BYTES)
    }

    /// Tenants that hold a reservation now.
    pub(crate) fn tenants(&self) -> usize {
        self.lock().len()
    }
}

/// Takes the permits of the first ask that has them now, else of the first
/// that gets them within `wait`.
async fn acquire(
    asks: [Option<(Arc<Semaphore>, u32)>; 2],
    wait: Duration,
) -> Option<OwnedSemaphorePermit> {
    for (semaphore, units) in asks.iter().flatten() {
        if let Ok(permit) = Arc::clone(semaphore).try_acquire_many_owned(*units) {
            return Some(permit);
        }
    }
    // A losing acquisition gives back what it was assigned when it drops.
    let [first, second] = asks;
    let waiting = async move {
        match (first, second) {
            (Some((a, a_units)), Some((b, b_units))) => tokio::select! {
                permit = a.acquire_many_owned(a_units) => permit,
                permit = b.acquire_many_owned(b_units) => permit,
            },
            (Some((only, units)), None) | (None, Some((only, units))) => {
                only.acquire_many_owned(units).await
            }
            (None, None) => std::future::pending().await,
        }
    };
    // The semaphores are never closed.
    tokio::time::timeout(wait, waiting).await.ok()?.ok()
}

/// A tenant's share of a reservation: released when it drops.
struct Claim {
    uploads: Arc<Uploads>,
    tenant: UploadTenant,
    bytes: u64,
}

impl Drop for Claim {
    fn drop(&mut self) {
        let mut tenants = self.uploads.lock();
        if let Some(held) = tenants.get_mut(&self.tenant) {
            held.bytes = held.bytes.saturating_sub(self.bytes);
            held.uploads = held.uploads.saturating_sub(1);
            if held.uploads == 0 {
                tenants.remove(&self.tenant);
            }
        }
    }
}

/// A body's share of the budget and of its tenant's caps, released when it
/// drops. The gateway holds it for as long as it holds the body.
pub(crate) struct Reservation {
    claim: Claim,
    permit: OwnedSemaphorePermit,
}

impl Reservation {
    /// Shrinks the reservation to `bytes`, the size of the body as buffered,
    /// when that is less than was reserved.
    pub(crate) fn settle(&mut self, bytes: u64) {
        if bytes >= self.claim.bytes {
            return;
        }
        {
            let mut tenants = self.claim.uploads.lock();
            if let Some(held) = tenants.get_mut(&self.claim.tenant) {
                held.bytes = held
                    .bytes
                    .saturating_sub(self.claim.bytes.saturating_sub(bytes));
            }
        }
        self.claim.bytes = bytes;
        let held = self.permit.num_permits();
        let excess = held.saturating_sub(units(bytes) as usize);
        drop(self.permit.split(excess));
    }
}

/// `bytes` in whole [`UNIT_BYTES`], rounded up.
fn units(bytes: u64) -> u32 {
    u32::try_from(bytes.div_ceil(UNIT_BYTES)).unwrap_or(u32::MAX)
}
