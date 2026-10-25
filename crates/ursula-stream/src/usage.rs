//! The per-stream usage ledger (bounded-state F20): what each stream
//! version committed, what ended streams committed until a consumer
//! acknowledged them, and what is retired into its bucket's total.
//!
//! A group's bucket totals are `retired + Σ live streams + Σ ended streams
//! not yet acknowledged`, so they equal the per-stream sums by
//! construction. The ledger starts in a group with the first
//! [`crate::StreamCommand::AckStreamUsage`]. Until then every write counts
//! straight into `retired`, as a group of Ursula 0.10.1 counts it into its
//! bucket, and nothing per stream is kept: a group snapshot, a log entry
//! and the replicated state stay those of 0.10.1.

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::model::BucketUsageSnapshot;
use crate::state_machine::COMMITTED_WRITE_UNIT_BYTES;

/// The monotonic part of usage: committed (non-deduplicated) appends.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedUsage {
    pub committed_append_bytes: u64,
    pub committed_records: u64,
    pub committed_write_units: u64,
}

impl CommittedUsage {
    /// One accepted write of `bytes` payload bytes and `records` records:
    /// its write units are rounded up and never below one.
    pub fn of_write(bytes: u64, records: u64) -> Self {
        Self {
            committed_append_bytes: bytes,
            committed_records: records,
            committed_write_units: bytes.div_ceil(COMMITTED_WRITE_UNIT_BYTES).max(1),
        }
    }

    /// Adds `other`, saturating.
    pub fn add(&mut self, other: Self) {
        self.committed_append_bytes = self
            .committed_append_bytes
            .saturating_add(other.committed_append_bytes);
        self.committed_records = self
            .committed_records
            .saturating_add(other.committed_records);
        self.committed_write_units = self
            .committed_write_units
            .saturating_add(other.committed_write_units);
    }
}

/// One stream version's usage (U-A). All zero until the group's ledger
/// starts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamUsage {
    pub committed_write_units: u64,
    pub committed_append_bytes: u64,
    pub committed_records: u64,
    /// `now_ms` of the stream's newest committed write (its create
    /// included); 0 when it wrote nothing since the ledger started.
    pub last_write_at_ms: u64,
    /// The group's usage sequence number at the stream's latest change of
    /// its counters or sizes; for an ended stream, at its end.
    pub usage_seq: u64,
}

impl StreamUsage {
    pub fn committed(&self) -> CommittedUsage {
        CommittedUsage {
            committed_append_bytes: self.committed_append_bytes,
            committed_records: self.committed_records,
            committed_write_units: self.committed_write_units,
        }
    }

    pub(crate) fn add_write(&mut self, write: CommittedUsage, now_ms: u64) {
        self.committed_append_bytes = self
            .committed_append_bytes
            .saturating_add(write.committed_append_bytes);
        self.committed_records = self
            .committed_records
            .saturating_add(write.committed_records);
        self.committed_write_units = self
            .committed_write_units
            .saturating_add(write.committed_write_units);
        self.last_write_at_ms = now_ms;
    }
}

/// How a stream version ended (U-B).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamEnd {
    /// A `DeleteStream` of a stream that had not expired.
    Deleted,
    /// Its TTL or expiry passed: the expiry sweep, a read or a write that
    /// found it expired, a create over it, or a delete after its expiry.
    Expired,
    /// Its bucket was purged.
    Purged,
}

/// The final usage of a stream version that ended, kept until a consumer
/// acknowledges it (U-B).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndedStreamUsage {
    pub stream_id: BucketStreamId,
    pub created_at_ms: u64,
    /// Its `Stream-Incarnation` token when it ended.
    pub incarnation: u64,
    pub content_type: String,
    /// Its final counters; `usage_seq` is the sequence number of its end,
    /// unique in the group.
    pub usage: StreamUsage,
    pub end: StreamEnd,
}

/// A bucket's retired usage in a [`StartedUsageLedger`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketCommittedUsage {
    pub bucket_id: String,
    pub usage: CommittedUsage,
}

/// A live stream's usage in a [`StartedUsageLedger`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamUsageEntry {
    pub stream_id: BucketStreamId,
    pub usage: StreamUsage,
}

/// A group's usage ledger as a group snapshot carries it, beside its
/// [`crate::StreamSnapshot`]. A backup does not carry it: a restored group starts
/// with its imported bucket totals retired and its ledger not started.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum UsageLedgerSnapshot {
    /// No acknowledgement started the ledger, or the snapshot was written
    /// by Ursula 0.10.1: the bucket totals of the stream snapshot are all
    /// retired.
    #[default]
    NotStarted,
    Started(StartedUsageLedger),
}

/// A started ledger. Every list is sorted: `retired` by bucket, `streams`
/// as the stream snapshot sorts its streams, `ended` by sequence number.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartedUsageLedger {
    /// The group's latest usage sequence number.
    pub usage_seq: u64,
    /// The highest sequence number a consumer acknowledged.
    pub acked_through: u64,
    pub retired: Vec<BucketCommittedUsage>,
    /// One entry per live stream: every live stream has one once the
    /// ledger started.
    pub streams: Vec<StreamUsageEntry>,
    pub ended: Vec<EndedStreamUsage>,
}

/// Why a started ledger does not fit the stream snapshot beside it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UsageLedgerMismatch {
    #[error("the usage ledger names stream '{0}', which the snapshot does not hold")]
    UnknownStream(BucketStreamId),
    #[error("the usage ledger names stream '{0}' twice")]
    DuplicateStream(BucketStreamId),
    #[error("the usage ledger has no entry for the snapshot's stream '{0}'")]
    MissingStream(BucketStreamId),
    #[error("usage sequence number {seq} is zero, repeated, or above the ledger's {usage_seq}")]
    InvalidSeq { seq: u64, usage_seq: u64 },
    #[error("ended stream usage at {seq} is at or below the acknowledged {acked_through}")]
    AcknowledgedEntry { seq: u64, acked_through: u64 },
    #[error(
        "bucket '{bucket_id}' totals {recorded:?}, but its retired, live and ended usage sum to \
         {parts:?}"
    )]
    Totals {
        bucket_id: String,
        recorded: CommittedUsage,
        parts: CommittedUsage,
    },
}

impl StartedUsageLedger {
    /// Checks the ledger against the stream snapshot it was written with,
    /// whose live streams are `live` and whose bucket totals are
    /// `bucket_usage`: one entry per live stream, distinct sequence numbers
    /// in range, nothing acknowledged still ended, and each bucket total
    /// equal to its retired, live and ended parts.
    pub fn check<'a>(
        &self,
        live: impl IntoIterator<Item = &'a BucketStreamId>,
        bucket_usage: &[BucketUsageSnapshot],
    ) -> Result<(), UsageLedgerMismatch> {
        let mut named = std::collections::HashSet::new();
        for entry in &self.streams {
            if !named.insert(&entry.stream_id) {
                return Err(UsageLedgerMismatch::DuplicateStream(
                    entry.stream_id.clone(),
                ));
            }
        }
        let mut held = std::collections::HashSet::new();
        for stream_id in live {
            if !named.contains(stream_id) {
                return Err(UsageLedgerMismatch::MissingStream(stream_id.clone()));
            }
            held.insert(stream_id);
        }
        if let Some(unknown) = named.iter().find(|stream_id| !held.contains(*stream_id)) {
            return Err(UsageLedgerMismatch::UnknownStream((*unknown).clone()));
        }

        let mut seqs = std::collections::HashSet::new();
        let live_seqs = self.streams.iter().map(|entry| entry.usage.usage_seq);
        let ended_seqs = self.ended.iter().map(|ended| ended.usage.usage_seq);
        for seq in live_seqs.chain(ended_seqs) {
            if seq == 0 || seq > self.usage_seq || !seqs.insert(seq) {
                return Err(UsageLedgerMismatch::InvalidSeq {
                    seq,
                    usage_seq: self.usage_seq,
                });
            }
        }
        if let Some(ended) = self
            .ended
            .iter()
            .find(|ended| ended.usage.usage_seq <= self.acked_through)
        {
            return Err(UsageLedgerMismatch::AcknowledgedEntry {
                seq: ended.usage.usage_seq,
                acked_through: self.acked_through,
            });
        }

        let mut parts = std::collections::BTreeMap::<&str, CommittedUsage>::new();
        for retired in &self.retired {
            parts
                .entry(retired.bucket_id.as_str())
                .or_default()
                .add(retired.usage);
        }
        for entry in &self.streams {
            parts
                .entry(entry.stream_id.bucket_id.as_str())
                .or_default()
                .add(entry.usage.committed());
        }
        for ended in &self.ended {
            parts
                .entry(ended.stream_id.bucket_id.as_str())
                .or_default()
                .add(ended.usage.committed());
        }
        let recorded = bucket_usage
            .iter()
            .map(|row| (row.bucket_id.as_str(), committed_of(row)))
            .collect::<std::collections::BTreeMap<_, _>>();
        for bucket_id in parts.keys().chain(recorded.keys()) {
            let part = parts.get(bucket_id).copied().unwrap_or_default();
            let total = recorded.get(bucket_id).copied().unwrap_or_default();
            if part != total {
                return Err(UsageLedgerMismatch::Totals {
                    bucket_id: (*bucket_id).to_owned(),
                    recorded: total,
                    parts: part,
                });
            }
        }
        Ok(())
    }
}

pub(crate) fn committed_of(row: &BucketUsageSnapshot) -> CommittedUsage {
    CommittedUsage {
        committed_append_bytes: row.usage.committed_append_bytes,
        committed_records: row.usage.committed_records,
        committed_write_units: row.usage.committed_write_units,
    }
}

/// The answer to an applied [`crate::StreamCommand::AckStreamUsage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UsageAck {
    /// The acknowledgement started the group's ledger: every live stream
    /// got a sequence number, up to `usage_seq`.
    Started { usage_seq: u64 },
    /// Every ended stream through `acked_through` is folded into its
    /// bucket's retired usage; `folded` of them by this acknowledgement.
    Acknowledged { acked_through: u64, folded: u64 },
    /// The acknowledgement named another history than the group's
    /// `history`, so it changed nothing: the consumer read another
    /// history's sequence numbers.
    OtherHistory { history: u64 },
}

/// Where a group's ledger stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageLedgerProgress {
    NotStarted,
    Started { usage_seq: u64, acked_through: u64 },
}

/// One stream version's latest usage in an export (U-C).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamUsageChange {
    pub stream_id: BucketStreamId,
    pub created_at_ms: u64,
    /// Its `Stream-Incarnation` token: in the group's current history for
    /// a live stream, at its end for an ended one.
    pub incarnation: u64,
    pub content_type: String,
    pub usage: StreamUsage,
    /// Bytes of `[retained, tail)`; 0 for an ended stream.
    pub retained_bytes: u64,
    /// Body length of its latest visible snapshot; 0 for an ended stream.
    pub snapshot_bytes: u64,
    /// How it ended, if it did.
    pub end: Option<StreamEnd>,
}

/// The changes of one group's ledger after a cursor, in sequence order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamUsageChanges {
    /// The group's history id, 0 before its genesis. Sequence numbers are
    /// of one history: a consumer whose cursor is of another restarts from
    /// 0.
    pub history: u64,
    pub progress: UsageLedgerProgress,
    /// The last sequence number these changes cover: every change in
    /// `(after, through]` is listed, each stream at its latest sequence
    /// number. Equal to the ledger's `usage_seq` when nothing was left out.
    pub through: u64,
    pub changes: Vec<StreamUsageChange>,
}
