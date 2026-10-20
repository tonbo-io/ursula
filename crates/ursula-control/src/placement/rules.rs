//! The rules that the closed loop's scripted negative traces (C6 of the
//! design, issue #768, and one for C2) turn off one at a time, so that each
//! trace shows its rule is what keeps the loop safe.
//!
//! Outside tests every rule holds: [`enforced`] is the constant `true` and
//! nothing can turn a rule off. Under `cfg(test)` a test turns one rule off
//! on its own thread with [`disable`], until the guard drops.

/// A rule a negative trace turns off. The planners, the views and the
/// dispatcher consult the first ten. The last two belong to the reconcile
/// shell and the removed-node sweep, which later slices build: until then
/// only the closed loop's model of them consults these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rule {
    /// C6(a): a voter change from a uniform config waits for a confirmed
    /// view.
    ConfirmedVoterChange,
    /// C6(a), a step outliving its leadership: a group whose voters reached
    /// the target reports only once no learner outside the target remains.
    LearnersBeforeReport,
    /// C6(c): a report applies only to the placement it names.
    PlacementCas,
    /// C6(d): both asks of a teardown pass only on a live leader's
    /// `NotMember`.
    RetireAnswer,
    /// C6(e): only a confirmed view creates a replica.
    ConfirmedCreation,
    /// C6(f): a history event drops every retirement.
    HistoryDrop,
    /// C6(g): a removed node closes only the replicas that stopped counting
    /// as voters.
    RemovedNodeCloses,
    /// C6(h): a view confirms only within the barrier's history and at its
    /// log id, never by index alone.
    ConfirmByLogId,
    /// C6(i): a seeded target only reports the voters it finds.
    SeededOnlyReports,
    /// C2: a leader that is no voter of its committed config waits (step 0
    /// of 5.3) instead of changing the voters from outside it.
    NotAMemberWaits,
    /// C6(b): a report follows a leadership proof.
    #[cfg(test)]
    LeadershipProof,
    /// C6(g): the removed-node sweep deletes a group's objects only on a
    /// live leader's `NotMember`.
    #[cfg(test)]
    SweepAnswer,
}

/// Whether `rule` holds: always, outside tests.
#[cfg(not(test))]
pub(crate) const fn enforced(_rule: Rule) -> bool {
    true
}

#[cfg(test)]
thread_local! {
    /// The rule this thread's test turned off, if any.
    static DISABLED: std::cell::Cell<Option<Rule>> = const { std::cell::Cell::new(None) };
}

/// Whether `rule` holds: unless this thread's test turned it off.
#[cfg(test)]
pub(crate) fn enforced(rule: Rule) -> bool {
    DISABLED.with(|disabled| disabled.get() != Some(rule))
}

/// Turns `rule` off on this thread until the guard drops.
#[cfg(test)]
pub(crate) fn disable(rule: Rule) -> Disabled {
    let previous = DISABLED.with(|disabled| disabled.replace(Some(rule)));
    Disabled { previous }
}

/// Turns the rule back on when it drops.
#[cfg(test)]
#[must_use = "the rule is on again as soon as the guard drops"]
pub(crate) struct Disabled {
    previous: Option<Rule>,
}

#[cfg(test)]
impl Drop for Disabled {
    fn drop(&mut self) {
        DISABLED.with(|disabled| disabled.set(self.previous));
    }
}
