//! When a learner has caught up: the round rule of the Raft thesis (4.2.1).
//!
//! A learner catches up in rounds. A round asks it to match the leader's
//! commit index as it stood when the round started. The learner is caught
//! up once a round ends within `bound` of its start, and stays so while the
//! round after it has not run past `bound`. Under steady writes the commit
//! index moves on while the learner replicates, so "matched the commit index
//! now" may never hold over a long round trip, while a round's index is
//! fixed and is matched one round trip later. A learner that cannot match
//! an index within `bound` of the time it was committed never catches up,
//! however long it replicates.
//!
//! Pure: the shell passes the time, the leader's commit index and each
//! learner's matched index.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_proto::placement::NodeId;

/// One learner's current round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Round {
    /// The leader's commit index when the round started.
    target: u64,
    started_ms: u64,
    /// Whether the round before this one ended within the bound. The
    /// learner's first round has none before it.
    kept_up: bool,
}

/// The catch-up rounds of the learners a group leader replicates to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatchUpRounds {
    rounds: BTreeMap<NodeId, Round>,
}

impl CatchUpRounds {
    /// Follows each of `learners` at `now_ms` and answers the ones caught
    /// up. `committed` is the leader's commit index and `matched` each
    /// member's matched index (`None` when it matches nothing). A learner's
    /// first round starts at its first observation. A round whose index the
    /// learner matched ends, and the next starts at the commit index now; a
    /// learner is caught up when its last round ended within `bound_ms` of
    /// its start and its current round has not run past `bound_ms`. A node
    /// that is not in `learners` loses its rounds.
    pub fn observe(
        &mut self,
        now_ms: u64,
        bound_ms: u64,
        committed: u64,
        matched: &BTreeMap<NodeId, Option<u64>>,
        learners: &BTreeSet<NodeId>,
    ) -> BTreeSet<NodeId> {
        self.rounds.retain(|node, _| learners.contains(node));
        let mut caught_up = BTreeSet::new();
        for learner in learners {
            let round = self.rounds.entry(*learner).or_insert(Round {
                target: committed,
                started_ms: now_ms,
                kept_up: false,
            });
            let elapsed = now_ms.saturating_sub(round.started_ms);
            let matched = matched.get(learner).copied().flatten();
            if matched.is_some_and(|matched| matched >= round.target) {
                *round = Round {
                    target: committed,
                    started_ms: now_ms,
                    kept_up: elapsed <= bound_ms,
                };
            } else if elapsed > bound_ms {
                round.kept_up = false;
            }
            if round.kept_up {
                caught_up.insert(*learner);
            }
        }
        caught_up
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use proptest::prelude::ProptestConfig;
    use proptest::prelude::Strategy;
    use proptest::prelude::prop;
    use proptest::prop_assert;
    use proptest::proptest;
    use ursula_proto::placement::NodeId;

    use super::CatchUpRounds;

    const BOUND_MS: u64 = 3000;

    fn learner() -> NodeId {
        NodeId::new(4)
    }

    fn learners() -> BTreeSet<NodeId> {
        BTreeSet::from([learner()])
    }

    fn observe(rounds: &mut CatchUpRounds, now_ms: u64, committed: u64, matched: u64) -> bool {
        rounds
            .observe(
                now_ms,
                BOUND_MS,
                committed,
                &BTreeMap::from([(learner(), Some(matched))]),
                &learners(),
            )
            .contains(&learner())
    }

    /// A learner always one entry behind the commit index, as under steady
    /// writes over a long round trip, is caught up once it matched the
    /// index of its round's start. The rule "matched the commit index now"
    /// never holds for it.
    #[test]
    fn a_learner_one_entry_behind_under_steady_writes_catches_up() {
        let mut rounds = CatchUpRounds::default();
        assert!(!observe(&mut rounds, 0, 100, 99), "the first round starts");
        assert!(observe(&mut rounds, 500, 120, 119), "it ends in time");
        assert!(observe(&mut rounds, 1000, 140, 139));
        assert!(observe(&mut rounds, 1500, 160, 159));
    }

    /// A learner that matches an index only later than the bound after it
    /// was committed never catches up, however long it replicates, and one
    /// that falls behind stops being caught up once its round runs late.
    #[test]
    fn a_learner_that_cannot_keep_up_is_never_caught_up() {
        let mut rounds = CatchUpRounds::default();
        // Each round's index is matched 4 s after the round started.
        observe(&mut rounds, 0, 1000, 0);
        for round in 1..=10_u64 {
            let now = round.saturating_mul(4000);
            let target = round.saturating_mul(1000);
            assert!(!observe(
                &mut rounds,
                now,
                target.saturating_add(1000),
                target
            ));
        }

        let mut kept = CatchUpRounds::default();
        assert!(observe(&mut kept, 0, 10, 10), "an idle learner keeps up");
        assert!(observe(&mut kept, 100, 10, 10));
        assert!(
            observe(&mut kept, 3000, 50, 9),
            "the round from 10 is not late yet"
        );
        assert!(
            !observe(&mut kept, 3200, 60, 9),
            "the round from 10 ran late"
        );
        assert!(!observe(&mut kept, 3300, 60, 60), "a late round ends");
        assert!(observe(&mut kept, 3400, 60, 60), "the next ends in time");
    }

    /// A node that is no longer a learner loses its rounds: back as a
    /// learner, it starts again.
    #[test]
    fn a_node_that_leaves_the_learners_starts_again() {
        let mut rounds = CatchUpRounds::default();
        assert!(observe(&mut rounds, 0, 10, 10));
        assert!(
            rounds
                .observe(100, BOUND_MS, 10, &BTreeMap::new(), &BTreeSet::new())
                .is_empty()
        );
        // A kept round from 10 would end here in time.
        assert!(!observe(&mut rounds, 200, 20, 15), "a first round again");
        assert!(observe(&mut rounds, 300, 20, 20));
    }

    /// One step of a trace: time passes, the commit index moves, and the
    /// learner's matched index moves up to the commit index.
    #[derive(Debug, Clone, Copy)]
    struct Step {
        after_ms: u64,
        commits: u64,
        replicates: u64,
    }

    fn steps() -> impl Strategy<Value = Vec<Step>> {
        prop::collection::vec(
            (0..2000_u64, 0..20_u64, 0..30_u64).prop_map(|(after_ms, commits, replicates)| Step {
                after_ms,
                commits,
                replicates,
            }),
            1..60,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Safety: whenever the learner is caught up, it has matched the
        /// commit index of an observation at most two bounds ago: the index
        /// of the start of a round that ended within the bound, at most one
        /// bound ago.
        #[test]
        fn a_caught_up_learner_matched_a_recent_commit_index(steps in steps()) {
            let mut rounds = CatchUpRounds::default();
            let (mut now, mut committed, mut matched) = (0_u64, 0_u64, 0_u64);
            let mut seen: Vec<(u64, u64)> = Vec::new();
            for step in steps {
                now = now.saturating_add(step.after_ms);
                committed = committed.saturating_add(step.commits);
                matched = matched.saturating_add(step.replicates).min(committed);
                seen.push((now, committed));
                if observe(&mut rounds, now, committed, matched) {
                    let recent = seen.iter().any(|(at, index)| {
                        now.saturating_sub(*at) <= BOUND_MS.saturating_mul(2) && matched >= *index
                    });
                    prop_assert!(recent, "caught up at {now} with {matched}: {seen:?}");
                }
            }
        }

        /// Liveness: a learner that, at every observation, has matched the
        /// commit index of the observation before, observed at most half a
        /// bound apart, is caught up from its second observation on,
        /// however fast the commit index moves.
        #[test]
        fn a_learner_one_observation_behind_is_caught_up(
            steps in prop::collection::vec((0..=BOUND_MS / 2, 1..50_u64), 2..60),
        ) {
            let mut rounds = CatchUpRounds::default();
            let (mut now, mut committed) = (0_u64, 0_u64);
            for (index, (after_ms, commits)) in steps.into_iter().enumerate() {
                let matched = committed;
                now = now.saturating_add(after_ms);
                committed = committed.saturating_add(commits);
                let caught_up = observe(&mut rounds, now, committed, matched);
                prop_assert!(caught_up == (index >= 1), "observation {index} at {now}");
            }
        }
    }
}
