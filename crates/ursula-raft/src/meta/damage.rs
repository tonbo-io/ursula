//! When the commit screen's refusal shows the meta replica damaged for
//! good (design 3.2).
//!
//! The commit screen refuses an append or a snapshot install that would
//! replace entries this replica knows are committed. It runs before
//! OpenRaft's vote check, so a deposed leader's conflicting append is
//! refused there too, once, and is no damage: its vote is below the
//! replica's own. A refusal is for good only when the request carries a
//! committed vote not below the replica's own, that is a current leader's:
//!
//! - on an append, `CommittedOverwrite`, the only refusal an append gets;
//! - on an install, what the gate records of a refused snapshot:
//!   `CommittedOverwrite`, or `UnrelatedHistory` against a history this
//!   replica could read.
//!
//! A current leader holds every committed entry, so its conflict with this
//! replica's committed entries means the replica holds entries the group no
//! longer has, as one that was down during an accepted meta plan holds
//! entries above its cut. Every other refusal, the transient ones above
//! all, never marks the replica damaged.

use ursula_runtime::GroupInfraError;

use crate::types::UrsulaVote;

/// The inbound request the commit screen refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Screened {
    Append,
    Install,
}

/// The commit screen's for-good refusal that damaged the meta replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitScreenDamage {
    pub refusal: GroupInfraError,
}

/// Whether the commit screen's `refusal` of a `screened` request carrying
/// `vote` damaged a replica whose own vote is `own` for good (see the
/// module docs).
pub(crate) fn commit_screen_damage(
    screened: Screened,
    refusal: &GroupInfraError,
    vote: &UrsulaVote,
    own: &UrsulaVote,
) -> bool {
    if !vote.is_committed()
        || !matches!(
            vote.partial_cmp(own),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        )
    {
        return false;
    }
    matches!(
        (screened, refusal),
        (
            Screened::Append | Screened::Install,
            GroupInfraError::CommittedOverwrite { .. }
        ) | (Screened::Install, GroupInfraError::UnrelatedHistory {
            local: Some(_),
            ..
        })
    )
}

#[cfg(test)]
mod tests {
    use ursula_runtime::GroupInfraError;
    use ursula_shard::RaftTarget;

    use super::Screened;
    use super::commit_screen_damage;
    use crate::types::UrsulaVote;

    fn overwrite() -> GroupInfraError {
        GroupInfraError::CommittedOverwrite {
            group: RaftTarget::Meta,
            committed: 9,
            index: 7,
        }
    }

    fn unrelated(local: Option<u64>) -> GroupInfraError {
        GroupInfraError::UnrelatedHistory {
            group: RaftTarget::Meta,
            history: 0xb2,
            replaced: 0xa1,
            local,
        }
    }

    /// The refusals that make meta damaged, and only those: a current
    /// leader's (a committed vote not below the replica's own)
    /// `CommittedOverwrite` on an append or an install, or its unrelated
    /// history against a history the replica read, on an install. A
    /// deposed leader's conflicting append, an uncommitted vote, a history
    /// the replica could not read, and the transient refusals never do.
    /// A classifier that ignored the vote, or took every refusal, fails it.
    #[test]
    fn only_a_current_leaders_conflict_damages_meta_for_good() {
        let own = UrsulaVote::new_committed(5, 2);
        let current = UrsulaVote::new_committed(5, 2);
        let newer = UrsulaVote::new_committed(6, 3);
        let deposed = UrsulaVote::new_committed(4, 1);
        let candidate = UrsulaVote::new(6, 3);

        for vote in [&current, &newer] {
            assert!(commit_screen_damage(
                Screened::Append,
                &overwrite(),
                vote,
                &own
            ));
            assert!(commit_screen_damage(
                Screened::Install,
                &overwrite(),
                vote,
                &own
            ));
            assert!(commit_screen_damage(
                Screened::Install,
                &unrelated(Some(0xc3)),
                vote,
                &own
            ));
            assert!(!commit_screen_damage(
                Screened::Install,
                &unrelated(None),
                vote,
                &own
            ));
            assert!(!commit_screen_damage(
                Screened::Append,
                &unrelated(Some(0xc3)),
                vote,
                &own
            ));
        }
        for vote in [&deposed, &candidate] {
            assert!(!commit_screen_damage(
                Screened::Append,
                &overwrite(),
                vote,
                &own
            ));
            assert!(!commit_screen_damage(
                Screened::Install,
                &overwrite(),
                vote,
                &own
            ));
            assert!(!commit_screen_damage(
                Screened::Install,
                &unrelated(Some(0xc3)),
                vote,
                &own
            ));
        }
        let transient = [
            GroupInfraError::LogBehindCommit {
                group: RaftTarget::Meta,
                committed: 9,
                held: Some(3),
            },
            GroupInfraError::RecoveryCutting {
                group: RaftTarget::Meta,
            },
            GroupInfraError::OwnerStopped,
        ];
        for refusal in &transient {
            assert!(!commit_screen_damage(
                Screened::Append,
                refusal,
                &current,
                &own
            ));
            assert!(!commit_screen_damage(
                Screened::Install,
                refusal,
                &current,
                &own
            ));
        }
    }
}
