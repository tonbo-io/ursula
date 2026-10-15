//! The measurements `ursula_stats` drains: per commit and per checkpoint of a bound database, at
//! most [`KEPT`] of each kind between drains.

use std::collections::VecDeque;
use std::time::Duration;

/// The most measurements of each kind a bound database keeps between two drains: 8,192 commits
/// take 384 KiB (48 bytes each), checkpoints 128 KiB. A power of two: the queues grow by
/// doubling, so their capacity stops at `KEPT` too.
pub(crate) const KEPT: usize = 8_192;

/// One commit logged at its commit point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommitStat {
    /// Frame bytes (compressed, as logged and sent), record bytes, and page images in the record.
    pub(crate) bytes: usize,
    pub(crate) raw: usize,
    pub(crate) pages: usize,
    /// The log write alone, and the whole commit point (wait, capture, encode, log write).
    pub(crate) log: Duration,
    pub(crate) vfs: Duration,
}

/// A bound database's measurements since the last drain, oldest first: the newest [`KEPT`]
/// commits and checkpoints. Past that, each new one drops the oldest of its kind, so a database
/// nobody drains holds a bounded amount, and the next drain counts what was dropped ([`Dropped`]).
#[derive(Debug, Default)]
pub(crate) struct Stats {
    commits: VecDeque<CommitStat>,
    /// How long each checkpoint held the checkpoint lock.
    checkpoints: VecDeque<Duration>,
    dropped: Dropped,
}

/// What [`Stats`] dropped since the last drain to keep at most [`KEPT`] of each kind. The dropped
/// commits' frame bytes are summed, so totals built from the drains count every commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Dropped {
    pub(crate) commits: u64,
    pub(crate) commit_bytes: u64,
    pub(crate) checkpoints: u64,
}

/// What a drain hands over: everything kept since the last one, oldest first, and what was dropped.
#[derive(Debug)]
pub(crate) struct Drained {
    pub(crate) commits: VecDeque<CommitStat>,
    pub(crate) checkpoints: VecDeque<Duration>,
    pub(crate) dropped: Dropped,
}

impl Stats {
    pub(crate) fn commit(&mut self, commit: CommitStat) {
        if let Some(oldest) = keep(&mut self.commits, commit) {
            let d = &mut self.dropped;
            d.commits = d.commits.saturating_add(1);
            d.commit_bytes = d.commit_bytes.saturating_add(oldest.bytes as u64);
        }
    }

    /// A checkpoint ended after holding the checkpoint lock for `held`.
    pub(crate) fn checkpoint(&mut self, held: Duration) {
        if keep(&mut self.checkpoints, held).is_some() {
            self.dropped.checkpoints = self.dropped.checkpoints.saturating_add(1);
        }
    }

    /// Hands everything over and starts again from nothing, which frees what it held.
    pub(crate) fn drain(&mut self) -> Drained {
        let Stats {
            commits,
            checkpoints,
            dropped,
        } = std::mem::take(self);
        Drained {
            commits,
            checkpoints,
            dropped,
        }
    }
}

/// Appends `item`, first dropping the oldest (returned) when `kept` already holds [`KEPT`].
fn keep<T>(kept: &mut VecDeque<T>, item: T) -> Option<T> {
    let oldest = if kept.len() >= KEPT {
        kept.pop_front()
    } else {
        None
    };
    kept.push_back(item);
    oldest
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::CommitStat;
    use super::Dropped;
    use super::KEPT;
    use super::Stats;

    /// A commit told apart by its frame bytes (`i`), with no record bytes, so a sum of the wrong
    /// field shows.
    fn commit(i: usize) -> CommitStat {
        CommitStat {
            bytes: i,
            raw: 0,
            pages: 1,
            log: Duration::ZERO,
            vfs: Duration::ZERO,
        }
    }

    // A drain hands over the newest commits, oldest first, and counts the older ones it dropped,
    // with their frame bytes. Then it starts again from nothing.
    #[test]
    fn a_drain_hands_over_the_newest_in_order_and_counts_the_rest() {
        let mut s = Stats::default();
        let total = KEPT + 4;
        for i in 0..total {
            s.commit(commit(i));
        }
        let d = s.drain();
        assert_eq!(d.commits.len(), KEPT);
        assert!(d.commits.iter().map(|c| c.bytes).eq(4..total));
        assert_eq!(d.dropped, Dropped {
            commits: 4,
            commit_bytes: (0..4).sum(),
            checkpoints: 0,
        });
        let d = s.drain();
        assert!(d.commits.is_empty() && d.checkpoints.is_empty());
        assert_eq!(d.dropped, Dropped::default());
        s.commit(commit(7));
        s.checkpoint(Duration::from_micros(8));
        let d = s.drain();
        assert!(d.commits.iter().map(|c| c.bytes).eq([7]));
        assert_eq!(d.checkpoints, [Duration::from_micros(8)]);
    }

    // A database nobody drains keeps at most `KEPT` of each kind, the newest, in queues no larger.
    #[test]
    fn a_database_nobody_drains_keeps_a_bounded_number_of_each_kind() {
        assert!(size_of::<CommitStat>() <= 64);
        let mut s = Stats::default();
        let total = 3 * KEPT;
        for i in 0..total {
            s.commit(commit(i));
            s.checkpoint(Duration::from_micros(i as u64));
            assert!(s.commits.len() <= KEPT);
            assert!(s.checkpoints.len() <= KEPT);
        }
        assert!(s.commits.capacity() <= KEPT, "{}", s.commits.capacity());
        assert!(s.checkpoints.capacity() <= KEPT);
        let d = s.drain();
        let newest = 2 * KEPT..total;
        assert!(d.commits.iter().map(|c| c.bytes).eq(newest.clone()));
        assert!(
            d.checkpoints
                .iter()
                .map(|c| c.as_micros() as usize)
                .eq(newest)
        );
        let dropped = 2 * KEPT as u64;
        assert_eq!(d.dropped, Dropped {
            commits: dropped,
            commit_bytes: (0..dropped).sum(),
            checkpoints: dropped,
        });
    }
}
