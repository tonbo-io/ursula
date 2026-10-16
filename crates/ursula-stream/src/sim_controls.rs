//! Defences a simulation's negative control turns off, to show that a DST
//! invariant catches their loss (`crates/ursula-sim`). The switches live in
//! this crate, which every crate with a defence depends on.
//!
//! Only `cfg(madsim)` builds can turn one off. madsim runs a simulation on
//! one OS thread, so the switches are thread-local and a simulation sees only
//! its own. Every other build has no switch: each query is a constant
//! `false`.

/// One defence of the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Defence {
    /// Row 11: a producer record from an earlier history closes its epoch.
    HistoryFence,
    /// G1: a command that names a `Stream-Incarnation` applies only to that
    /// incarnation.
    IncarnationCheck,
    /// An import of a backup the group was already restored from answers
    /// `already` instead of rewinding again.
    ImportKey,
    /// A recovery plan that cuts the logs cuts them inside its accept job,
    /// through an in-process restart of the group, which also demotes a
    /// replica that leads or campaigns. Off, there is no such restart and
    /// the cut waits for the member's next start, as before the #581 review
    /// (B1).
    RecoveryCut,
    /// A member of a recovery plan refuses the plan's leader while its log
    /// holds an entry above the point at or below the plan term (M1 of the
    /// second #581 review).
    PlanLogGuard,
    /// A replica holding a recovery plan takes replication only from a
    /// leader above the plan term (M1 of the second #581 review).
    PlanReplicationFence,
    /// A write whose body was read never redirects: a leadership refusal
    /// after the body answers `503` with `Retry-After` (#581 section 6.1).
    /// Off, the HTTP front door answers it as it would before the body, with
    /// a `307` to the leader its hint names.
    NoRedirectAfterBody,
    /// An immutable-read chunk that ends at the tail of an open stream is
    /// never marked, even when it fills its chunk (#581 section 5.3).
    ImmutableOpenTail,
    /// A follower answers an immutable-read chunk only when it can mark it,
    /// and forwards the others to the leader. Off, it answers every chunk
    /// from its own state, as a base-protocol read, which hides its own open
    /// tail (#567 review finding 4).
    FollowerMarkCheck,
    /// A leader linearizes an immutable-read chunk it cannot mark, and a
    /// `416`, before it answers (#581 section 5.3, m1 of the step 5 review).
    ImmutableLeaderBarrier,
}

#[cfg(madsim)]
mod switches {
    use std::cell::Cell;

    use super::Defence;

    std::thread_local! {
        static OFF: Cell<[bool; 10]> = const { Cell::new([false; 10]) };
        static FIRED: Cell<[u64; 10]> = const { Cell::new([0; 10]) };
    }

    fn slot(defence: Defence) -> usize {
        match defence {
            Defence::HistoryFence => 0,
            Defence::IncarnationCheck => 1,
            Defence::ImportKey => 2,
            Defence::RecoveryCut => 3,
            Defence::PlanLogGuard => 4,
            Defence::PlanReplicationFence => 5,
            Defence::NoRedirectAfterBody => 6,
            Defence::ImmutableOpenTail => 7,
            Defence::FollowerMarkCheck => 8,
            Defence::ImmutableLeaderBarrier => 9,
        }
    }

    pub(super) fn is_off(defence: Defence) -> bool {
        OFF.with(|off| off.get().get(slot(defence)).copied().unwrap_or(false))
    }

    pub(super) fn note_fired(defence: Defence) {
        FIRED.with(|fired| {
            let mut all = fired.get();
            if let Some(entry) = all.get_mut(slot(defence)) {
                *entry = entry.saturating_add(1);
            }
            fired.set(all);
        });
    }

    pub(super) fn fired(defence: Defence) -> u64 {
        FIRED.with(|fired| fired.get().get(slot(defence)).copied().unwrap_or(0))
    }

    pub(super) fn reset_fired() {
        FIRED.with(|fired| fired.set([0; 10]));
    }

    pub(super) fn set_off(defence: Defence, value: bool) {
        OFF.with(|off| {
            let mut all = off.get();
            if let Some(entry) = all.get_mut(slot(defence)) {
                *entry = value;
            }
            off.set(all);
        });
    }
}

/// Whether a negative control turned `defence` off on this thread.
#[inline]
pub fn off(defence: Defence) -> bool {
    #[cfg(madsim)]
    {
        switches::is_off(defence)
    }
    #[cfg(not(madsim))]
    {
        let _ = defence;
        false
    }
}

/// Turns `defence` off (or back on) for this thread's simulation.
#[cfg(madsim)]
pub fn set_off(defence: Defence, value: bool) {
    switches::set_off(defence, value);
}

/// Records that `defence` decided an outcome on this thread: a negative
/// control's sibling test reads it to show which defence fired.
#[inline]
pub fn note_fired(defence: Defence) {
    #[cfg(madsim)]
    {
        switches::note_fired(defence);
    }
    #[cfg(not(madsim))]
    {
        let _ = defence;
    }
}

/// How often `defence` decided an outcome on this thread since
/// [`reset_fired`].
#[cfg(madsim)]
pub fn fired(defence: Defence) -> u64 {
    switches::fired(defence)
}

/// Forgets what fired on this thread, before a simulation.
#[cfg(madsim)]
pub fn reset_fired() {
    switches::reset_fired();
}
