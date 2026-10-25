//! The leadership a cold upload is fenced to (bounded-stream-state F14j).
//!
//! A leader uploads cold objects of its own: a flushed chunk or pack, a
//! compaction's replacement, an external payload, a snapshot body and a
//! parts commit's copy. It takes its group's committed leader, term and node,
//! at the check that admits the upload, before the first byte of the PUT.
//! The proposal that names the object as new must still be appended under
//! that leader. The Raft engine hands it to OpenRaft with `with_leader`, and
//! OpenRaft refuses it before it appends anything once the leadership
//! changed, a re-election of the same node included. So an entry that names
//! an object one leadership uploaded commits only under that leadership.
//!
//! - [`UploadFence`]: the leadership an upload was planned under.
//! - [`ProposedUpload`]: the one value through which an upload's proposals
//!   reach its fence.
//! - [`uploaded_object`]: the object a command names as new, which the Raft
//!   engine refuses to propose without a fence.
//! - `fence_ledger` (madsim only): the fence of each proposed upload and the
//!   leadership each applied naming carries, for the DST scenario
//!   `cold_uploads_stay_fenced`.

use std::sync::Arc;

use ursula_stream::StreamCommand;

use crate::engine::GroupEngineError;
use crate::engine::LeadershipReason;
use crate::engine::LeadershipRefusal;
use crate::engine::Proposal;

/// A replicated group's leadership as OpenRaft's committed leader id names
/// it: the term and the node that won it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommittedLeader {
    term: u64,
    node_id: u64,
}

impl CommittedLeader {
    /// The leadership `node_id` won in `term`.
    ///
    /// Not stable API: the Raft engine and tests build it, and F2 replaces
    /// its construction outside them, so a 0.9.x release may remove it.
    #[doc(hidden)]
    pub fn new(term: u64, node_id: u64) -> Self {
        Self { term, node_id }
    }

    pub fn term(self) -> u64 {
        self.term
    }

    pub fn node_id(self) -> u64 {
        self.node_id
    }
}

/// The leadership a cold upload was planned under, which its proposal must
/// still hold when OpenRaft appends it. It lives in memory only: no entry,
/// state or RPC carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UploadFence {
    /// An engine without replication, which leads in one leadership for as
    /// long as it runs. A Raft engine refuses an upload fenced to it.
    Unreplicated,
    /// A replicated group's committed leader.
    Leader(CommittedLeader),
}

impl UploadFence {
    /// The term of the leadership: `0` without replication.
    pub fn term(self) -> u64 {
        match self {
            Self::Unreplicated => 0,
            Self::Leader(leader) => leader.term,
        }
    }
}

/// One upload that the requests proposing it share, with the leadership it
/// is fenced to. A request names it through an `Arc`: an exclusive flush, a
/// compaction and an external write propose it once, and the slices of a
/// pack and a snapshot publish with its JSON-boundary retry propose the one
/// value several times. A Raft engine reaches the fence of an upload
/// proposal only through it.
///
/// An upload request takes a `ProposedUpload`, never a raw fence:
///
/// ```compile_fail,E0308
/// let request = ursula_runtime::AppendRequest::from_bytes(
///     ursula_shard::BucketStreamId::new("bucket", "stream"),
///     b"payload".to_vec(),
/// );
/// let payload = ursula_runtime::ExternalPayloadRef::default();
/// let fence = ursula_runtime::UploadFence::Unreplicated;
/// let _ = ursula_runtime::AppendExternalRequest::from_append_request(request, payload, 0, fence);
/// ```
///
/// It is not `Clone`: the requests of one upload share the one value
/// through its `Arc` instead of each holding a copy.
///
/// ```compile_fail,E0277
/// fn shared<T: Clone>(_: &T) {}
/// let upload = ursula_runtime::ProposedUpload::new(ursula_runtime::UploadFence::Unreplicated);
/// shared(&*upload);
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct ProposedUpload {
    fence: UploadFence,
}

impl ProposedUpload {
    /// The upload a node plans under `fence`, before it writes the first
    /// byte of the object.
    ///
    /// Not stable API: F2 replaces it with the hold an upload takes before
    /// its PUT, so a 0.9.x release may remove it.
    #[doc(hidden)]
    pub fn new(fence: UploadFence) -> Arc<Self> {
        Arc::new(Self { fence })
    }

    /// The leadership the upload's proposals must still hold.
    pub fn fence(&self) -> UploadFence {
        self.fence
    }
}

/// The refusal of an upload whose node does not lead under a fence it can
/// propose with: nothing was proposed, so its uploader deletes the object.
pub fn upload_not_leading() -> GroupEngineError {
    GroupEngineError::NotLeader(LeadershipRefusal {
        leader: None,
        proposal: Proposal::NotProposed,
        reason: LeadershipReason::Follower,
    })
}

/// The object `command` names as new that its proposer uploaded, if it names
/// one: such a command is proposed only with the fence of its upload. The
/// match lists every command, so a new upload command cannot be proposed
/// without one by accident.
pub fn uploaded_object(command: &StreamCommand) -> Option<&str> {
    match command {
        StreamCommand::CreateExternal {
            initial_payload, ..
        } => Some(&initial_payload.s3_path),
        StreamCommand::AppendExternal { payload, .. } => Some(&payload.s3_path),
        StreamCommand::PublishSnapshotExternal { object, .. } => Some(&object.s3_path),
        StreamCommand::FlushCold { chunk, .. } => Some(&chunk.s3_path),
        StreamCommand::CompactCold { replacement, .. } => Some(&replacement.s3_path),
        StreamCommand::IfIncarnation { command, .. } => uploaded_object(command),
        // An import names the operator's cold copy, which no leadership of
        // this group uploaded. The rest carry their bytes inline or name
        // objects that earlier entries committed.
        StreamCommand::CreateBucket { .. }
        | StreamCommand::CreateStream { .. }
        | StreamCommand::Append { .. }
        | StreamCommand::PublishSnapshot { .. }
        | StreamCommand::AdvanceRetention { .. }
        | StreamCommand::TouchStreamAccess { .. }
        | StreamCommand::Close { .. }
        | StreamCommand::DeleteStream { .. }
        | StreamCommand::PurgeBucket { .. }
        | StreamCommand::AckColdGc { .. }
        | StreamCommand::AckStreamUsage { .. }
        | StreamCommand::DeferColdGc { .. }
        | StreamCommand::ImportSnapshot { .. }
        | StreamCommand::NewHistory { .. }
        | StreamCommand::TidyStream { .. }
        | StreamCommand::ExpireStreams { .. }
        | StreamCommand::OffloadColdRefs { .. } => None,
    }
}

/// Under madsim: the fence each upload was proposed under and every applied
/// entry that names an upload under another leadership. A DST scenario arms
/// it, runs, and reads the namings that broke the fence. madsim runs a
/// simulation on one thread, so the ledger is thread-local and sees every
/// replica of the simulation. It records nothing while it is not armed.
#[cfg(madsim)]
pub mod fence_ledger {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use ursula_stream::StreamCommand;

    use super::CommittedLeader;
    use super::uploaded_object;

    /// An applied entry that names an upload under a leadership other than
    /// the one the upload was fenced to.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct UnfencedNaming {
        pub object: String,
        pub fenced_to: CommittedLeader,
        pub applied_under: CommittedLeader,
    }

    #[derive(Default)]
    struct Ledger {
        armed: bool,
        proposed: BTreeMap<String, CommittedLeader>,
        broken: Vec<UnfencedNaming>,
    }

    std::thread_local! {
        static LEDGER: RefCell<Ledger> = RefCell::new(Ledger::default());
    }

    /// Starts recording, from an empty ledger.
    pub fn arm() {
        LEDGER.with(|ledger| {
            *ledger.borrow_mut() = Ledger {
                armed: true,
                ..Ledger::default()
            };
        });
    }

    /// Stops recording and returns the namings that broke their fence.
    pub fn disarm() -> Vec<UnfencedNaming> {
        LEDGER.with(|ledger| std::mem::take(&mut *ledger.borrow_mut()).broken)
    }

    /// The upload of `object` is proposed fenced to `leader`.
    pub fn note_proposed(object: &str, leader: CommittedLeader) {
        LEDGER.with(|ledger| {
            let mut ledger = ledger.borrow_mut();
            if ledger.armed {
                ledger.proposed.insert(object.to_owned(), leader);
            }
        });
    }

    /// A replica applies `command`, committed under `leader`.
    pub fn note_applied(command: &StreamCommand, leader: CommittedLeader) {
        let Some(object) = uploaded_object(command) else {
            return;
        };
        LEDGER.with(|ledger| {
            let mut ledger = ledger.borrow_mut();
            if !ledger.armed {
                return;
            }
            let Some(&fenced_to) = ledger.proposed.get(object) else {
                return;
            };
            if fenced_to != leader {
                ledger.broken.push(UnfencedNaming {
                    object: object.to_owned(),
                    fenced_to,
                    applied_under: leader,
                });
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use ursula_shard::BucketStreamId;
    use ursula_stream::ColdChunkRef;
    use ursula_stream::ExternalPayloadRef;
    use ursula_stream::StreamCommand;

    use super::CommittedLeader;
    use super::ProposedUpload;
    use super::UploadFence;
    use super::uploaded_object;

    fn stream() -> BucketStreamId {
        BucketStreamId::new("bucket", "stream")
    }

    fn payload(path: &str) -> ExternalPayloadRef {
        ExternalPayloadRef {
            s3_path: path.to_owned(),
            payload_len: 1,
            object_size: 1,
        }
    }

    fn chunk(path: &str) -> ColdChunkRef {
        ColdChunkRef {
            start_offset: 0,
            end_offset: 1,
            s3_path: path.to_owned(),
            object_size: 1,
            object_offset: 0,
            shared_object: false,
            payload_digest: String::new(),
        }
    }

    #[test]
    fn uploaded_object_names_every_upload_command() {
        let uploads = [
            (
                StreamCommand::CreateExternal {
                    stream_id: stream(),
                    content_type: "application/octet-stream".to_owned(),
                    initial_payload: payload("created"),
                    message_count: 0,
                    close_after: false,
                    stream_seq: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    now_ms: 0,
                },
                "created",
            ),
            (
                StreamCommand::AppendExternal {
                    stream_id: stream(),
                    content_type: None,
                    payload: payload("appended"),
                    message_count: 0,
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                },
                "appended",
            ),
            (
                StreamCommand::PublishSnapshotExternal {
                    stream_id: stream(),
                    snapshot_offset: 0,
                    content_type: "application/octet-stream".to_owned(),
                    object: payload("snapshot"),
                    digest: String::new(),
                    now_ms: 0,
                    expected_incarnation: None,
                },
                "snapshot",
            ),
            (
                StreamCommand::FlushCold {
                    stream_id: stream(),
                    chunk: chunk("flushed"),
                    cold_generation: 0,
                },
                "flushed",
            ),
            (
                StreamCommand::CompactCold {
                    stream_id: stream(),
                    old_chunks: vec![chunk("old")],
                    replacement: chunk("replacement"),
                    gc_not_before_ms: 0,
                },
                "replacement",
            ),
        ];
        for (command, object) in uploads {
            assert_eq!(uploaded_object(&command), Some(object), "{command:?}");
            let wrapped = StreamCommand::IfIncarnation {
                incarnation: 7,
                command: Box::new(command),
            };
            assert_eq!(uploaded_object(&wrapped), Some(object), "{wrapped:?}");
        }
        let inline = StreamCommand::Append {
            stream_id: stream(),
            content_type: None,
            payload: bytes::Bytes::from_static(b"inline"),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        };
        assert_eq!(uploaded_object(&inline), None);
        let offload = StreamCommand::OffloadColdRefs {
            stream_id: stream(),
            refs: Vec::new(),
        };
        assert_eq!(uploaded_object(&offload), None);
    }

    #[test]
    fn a_proposed_upload_carries_its_fence_and_the_term_of_no_replication_is_zero() {
        let leader = CommittedLeader::new(4, 2);
        let upload = ProposedUpload::new(UploadFence::Leader(leader));
        assert_eq!(upload.fence(), UploadFence::Leader(leader));
        assert_eq!(upload.fence().term(), 4);
        assert_eq!(UploadFence::Unreplicated.term(), 0);
    }
}
