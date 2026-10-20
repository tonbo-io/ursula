use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::history::HistoryEvent;
use crate::history::InheritedHistory;
use crate::model::BucketUsageSnapshot;
use crate::model::ColdChunkRef;
use crate::model::ColdGcEntry;
use crate::model::HotPayloadSegment;
use crate::model::ObjectPayloadRef;
use crate::model::ProducerSnapshot;
use crate::model::StreamMetadata;
use crate::model::StreamVisibleSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSnapshot {
    /// Backup format of this snapshot ([`crate::BACKUP_FORMAT_VERSION`]).
    /// It is the first field in every backup format from 5 on, so
    /// [`decode_backup_snapshot`] reads it before anything else of the
    /// document. Backup formats up to 4 wrote a map with this field under
    /// the key `format_epoch`, and Ursula 0.5.x none.
    pub format_version: u32,
    pub buckets: Vec<String>,
    /// Permanent bucket-erasure fences.
    pub erased_buckets: Vec<String>,
    pub streams: Vec<StreamSnapshotEntry>,
    pub pending_cold_gc: Vec<ColdGcEntry>,
    pub next_cold_gc_seq: u64,
    /// Monotonic per-bucket usage counters.
    pub bucket_usage: Vec<BucketUsageSnapshot>,
    /// Largest `created_at_ms` this group assigned (C7, F14g).
    pub last_created_at_ms: u64,
    /// The group's latest history event; `None` before its genesis.
    pub history: Option<HistoryEvent>,
}

/// An empty snapshot of this binary's backup format. Fixtures restore from it.
impl Default for StreamSnapshot {
    fn default() -> Self {
        Self {
            format_version: crate::BACKUP_FORMAT_VERSION,
            buckets: Vec::new(),
            erased_buckets: Vec::new(),
            streams: Vec::new(),
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
            bucket_usage: Vec::new(),
            last_created_at_ms: 0,
            history: None,
        }
    }
}

impl StreamSnapshot {
    /// Refuses a snapshot document of another backup format before anything
    /// else of it is trusted. `found` is its backup format, `None` when it has
    /// none (Ursula 0.5.x).
    pub fn check_format(found: Option<u32>) -> Result<(), StreamSnapshotError> {
        match found {
            Some(crate::BACKUP_FORMAT_VERSION) => Ok(()),
            found => Err(StreamSnapshotError::BackupFormat { found }),
        }
    }
}

/// Why a backup group object does not restore.
#[derive(Debug, thiserror::Error)]
pub enum BackupSnapshotError {
    /// The document does not decode as a stream snapshot.
    #[error("backup snapshot does not decode: {0}")]
    Undecodable(#[source] crate::msgpack::DecodeError),
    /// The document is of another backup format.
    #[error(transparent)]
    Format(#[from] StreamSnapshotError),
}

/// Decodes a backup group object (E10). It reads the document's backup
/// format first and refuses one of another format before it decodes the
/// rest under this format's rules: under compact MessagePack a document of
/// another format whose field count happens to match would otherwise be
/// misread. The import route, its cold check, `ursulactl` and the
/// simulation share this one check.
pub fn decode_backup_snapshot(bytes: &[u8]) -> Result<StreamSnapshot, BackupSnapshotError> {
    let BackupFormatProbe(found) =
        crate::msgpack::decode(bytes).map_err(BackupSnapshotError::Undecodable)?;
    StreamSnapshot::check_format(found)?;
    crate::msgpack::decode(bytes).map_err(BackupSnapshotError::Undecodable)
}

/// The backup format of a group object of any backup format: the first
/// element of the array of backup format 5 and later, the `format_epoch`
/// entry of the map of backup formats up to 4, `None` for a map without
/// one (Ursula 0.5.x). It skips the rest of the document unread.
struct BackupFormatProbe(Option<u32>);

impl<'de> Deserialize<'de> for BackupFormatProbe {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(BackupFormatProbeVisitor)
    }
}

struct BackupFormatProbeVisitor;

impl<'de> serde::de::Visitor<'de> for BackupFormatProbeVisitor {
    type Value = BackupFormatProbe;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a stream snapshot")
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let found = seq
            .next_element::<u32>()?
            .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
        while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
        Ok(BackupFormatProbe(Some(found)))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut found = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == "format_epoch" && found.is_none() {
                found = Some(map.next_value::<u32>()?);
            } else {
                map.next_value::<serde::de::IgnoredAny>()?;
            }
        }
        Ok(BackupFormatProbe(found))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSnapshotEntry {
    pub metadata: StreamMetadata,
    pub hot_start_offset: u64,
    pub payload: Vec<u8>,
    pub hot_segments: Vec<HotPayloadSegment>,
    pub cold_index_generation: u64,
    pub cold_chunks: Vec<ColdChunkRef>,
    pub external_segments: Vec<ObjectPayloadRef>,
    /// Independent destructive-retention floor.
    pub retained_offset: u64,
    pub visible_snapshot: Option<StreamVisibleSnapshot>,
    pub producer_states: Vec<ProducerSnapshot>,
    /// The stream's epoch floor: the highest epoch of any producer record it
    /// retired.
    pub retired_epoch_watermark: Option<u64>,
    /// The group's history id when the stream was created or last written.
    pub history: u64,
    /// What the stream's current history began from, recorded by its first
    /// write after a history event.
    pub inherited: Option<InheritedHistory>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum StreamSnapshotError {
    #[error("snapshot contains duplicate bucket '{0}'")]
    DuplicateBucket(String),
    #[error("snapshot contains duplicate erased bucket '{0}'")]
    DuplicateErasedBucket(String),
    #[error("snapshot bucket '{0}' is both active and erased")]
    ActiveBucketErased(String),
    #[error("snapshot contains duplicate stream '{0}'")]
    DuplicateStream(BucketStreamId),
    #[error("snapshot stream '{stream_id}' contains duplicate producer '{producer_id}'")]
    DuplicateProducer {
        stream_id: BucketStreamId,
        producer_id: String,
    },
    #[error("snapshot stream '{0}' references a missing bucket")]
    MissingBucket(BucketStreamId),
    #[error(
        "snapshot stream '{stream_id}' tail offset {tail_offset} does not match payload length {payload_len}"
    )]
    PayloadLengthMismatch {
        stream_id: BucketStreamId,
        tail_offset: u64,
        payload_len: u64,
    },
    #[error(
        "snapshot stream '{stream_id}' visible snapshot offset {snapshot_offset} is beyond tail offset {tail_offset}"
    )]
    SnapshotOffsetOutOfRange {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        tail_offset: u64,
    },
    #[error("{}", snapshot_backup_format_message(.found))]
    BackupFormat { found: Option<u32> },
    #[error("snapshot cold GC entry {seq} has no owning bucket")]
    UnattributedColdGc { seq: u64 },
    #[error("snapshot cold GC entry {seq} targets a stream without a cold generation")]
    ColdGcWithoutGeneration { seq: u64 },
}

fn snapshot_backup_format_message(found: &Option<u32>) -> String {
    let found = match found {
        Some(found) => format!("is backup format {found}"),
        None => "has no format_epoch field".to_owned(),
    };
    crate::backup_format_refusal("snapshot", &found)
}

#[cfg(test)]
mod tests {
    use super::BackupSnapshotError;
    use super::StreamSnapshot;
    use super::StreamSnapshotError;
    use super::decode_backup_snapshot;
    use crate::msgpack;

    fn of_format(format_version: u32) -> Vec<u8> {
        msgpack::encode(&StreamSnapshot {
            format_version,
            buckets: vec!["tenant-a".to_owned()],
            ..StreamSnapshot::default()
        })
        .unwrap()
    }

    /// E10: a group object of this backup format decodes, and one of any
    /// other is refused by its format before the rest is read: a compact
    /// document of another format, the named map of backup format 4 (Ursula
    /// 0.8) and a map with no format at all (Ursula 0.5.x).
    #[test]
    fn a_backup_of_another_format_is_refused_by_its_format() {
        let snapshot = decode_backup_snapshot(&of_format(crate::BACKUP_FORMAT_VERSION)).unwrap();
        assert_eq!(snapshot.buckets, vec!["tenant-a".to_owned()]);

        let older = crate::BACKUP_FORMAT_VERSION.checked_sub(1).unwrap();
        let newer = crate::BACKUP_FORMAT_VERSION.checked_add(1).unwrap();
        let named_0_8 = msgpack::encode(&serde_json::json!({
            "format_epoch": 4,
            "buckets": ["tenant-a"],
            "streams": [],
        }))
        .unwrap();
        let without_format =
            msgpack::encode(&serde_json::json!({"buckets": ["tenant-a"], "streams": []})).unwrap();
        for (body, found) in [
            (of_format(older), Some(older)),
            (of_format(newer), Some(newer)),
            (named_0_8, Some(4)),
            (without_format, None),
        ] {
            let error = decode_backup_snapshot(&body).unwrap_err();
            assert!(
                matches!(
                    &error,
                    BackupSnapshotError::Format(StreamSnapshotError::BackupFormat { found: got })
                        if *got == found
                ),
                "{error}"
            );
        }

        // A document that is no snapshot at all does not decode.
        for body in [vec![0x90], vec![0xc0], vec![0x91, 0xa1, b'x']] {
            assert!(matches!(
                decode_backup_snapshot(&body),
                Err(BackupSnapshotError::Undecodable(_))
            ));
        }
    }
}
