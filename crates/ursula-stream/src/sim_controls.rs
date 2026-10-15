//! Defences a simulation's negative control turns off, to show that a DST
//! invariant catches their loss (`crates/ursula-sim`, history scenarios).
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
}

#[cfg(madsim)]
mod switches {
    use std::cell::Cell;

    use super::Defence;

    std::thread_local! {
        static OFF: Cell<[bool; 6]> = const { Cell::new([false; 6]) };
    }

    fn slot(defence: Defence) -> usize {
        match defence {
            Defence::HistoryFence => 0,
            Defence::IncarnationCheck => 1,
            Defence::ImportKey => 2,
            Defence::RecoveryCut => 3,
            Defence::PlanLogGuard => 4,
            Defence::PlanReplicationFence => 5,
        }
    }

    pub(super) fn is_off(defence: Defence) -> bool {
        OFF.with(|off| off.get().get(slot(defence)).copied().unwrap_or(false))
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
