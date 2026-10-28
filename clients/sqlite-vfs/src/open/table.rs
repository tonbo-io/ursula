//! The open table (design: Open) as one pure function: what the open does with the local
//! generation it found and the HEAD it got. [`decide`] does no I/O; the open carries out its
//! [`OpenRow`].
//!
//! | local generation | HEAD | row |
//! | --- | --- | --- |
//! | any | no `Ursula-Protocol` | refuse `no_server_identity` |
//! | any | `Ursula-Protocol` of another release line | refuse `protocol_mismatch` |
//! | trusted | another `Ursula-Cluster-Id` | refuse `wrong_cluster` |
//! | none, replaced | `404` | create |
//! | trusted, holding no data | `404` | create |
//! | trusted, holding data | `404` | refuse `not_found`, or create with `recreate` |
//! | any | another content type | refuse `stream_format` |
//! | any | closed | refuse `stream_state` |
//! | none | `200` | build |
//! | replaced | `200` | build, the generation salvaged (`replaced`) |
//! | trusted, same token, holding no data, a snapshot past its ACK | `200` | build, the generation trashed |
//! | trusted, same token | `200` | resume |
//! | trusted, other token, inheriting, nothing pending | `200` | re-key ([`inherits`]) |
//! | trusted, other token, otherwise, holding data | `200` | build, the generation salvaged |
//! | trusted, other token, otherwise, holding no data | `200` | build, the generation trashed |
//!
//! Rows match in order, so a refusal sends nothing and moves no file. A generation is replaced
//! when its db file is not its own (page 1) or its log's first segment header is unreadable; it
//! is salvaged as found, then the open goes on as with none. A generation salvaged after a
//! token change is `recreated` when this open created the stream (`recreate`), and
//! `history_changed` otherwise. A generation whose history inherits it while its log holds
//! commits after the latest ACK is salvaged too, never re-keyed: an ACK record trails the
//! stream's acknowledgements, so those commits may include acknowledged ones the history lost, and
//! a re-keyed log names the new token, so a later open would resume it and discard them.
//! Rows match in order, so a refusal sends nothing and moves no file.
//!
//! A generation of the stream's token holding no data whose latest ACK is below the stream's
//! latest snapshot is built over rather than resumed ([`behind_snapshot`]): it has nothing to
//! keep, and the stream below that snapshot may be the claim of an import, which no read takes
//! (`codec::stream::StreamError::ImportIncomplete`), whereas a build starts at the snapshot. The
//! row is decided before anything is sent, so a generation whose claim lost to an import's, or
//! that never claimed, claims only after its build, once. One holding data resumes: an import
//! refuses a stream holding anything, so such a generation cannot lie below an import's snapshot,
//! and if it does, its reconcile meets the import's claim and resets `stream_state`.
//!
//! When the open's first HEAD got no answer with nothing sent (the stream is unreachable, design:
//! Open), [`decide_unreachable`] decides instead:
//!
//! | local generation | row |
//! | --- | --- |
//! | trusted, with a CLAIM record | open provisionally, the claim deferred behind a pre-check |
//! | trusted, without one | wait: retry until the open's signal fires |
//! | none | wait |
//!
//! A reader's open (design: Readers) decides by [`decide_reader`] instead, after the same checks of
//! the HEAD ([`check_head`]). A reader generation holds no commit of its own, only what it read, so
//! it is never salvaged and never re-keyed: anything the stream no longer extends is built anew. A
//! trusted reader generation of another cluster is refused as an owner's is (the stream it reads is
//! not the one it caught up with), and a reader with none builds, having nothing to compare. A
//! closed stream is read like any other: a reader writes nothing to it.
//!
//! | reader generation | HEAD | row |
//! | --- | --- | --- |
//! | any | no `Ursula-Protocol` | refuse `no_server_identity` |
//! | any | `Ursula-Protocol` of another release line | refuse `protocol_mismatch` |
//! | trusted | another `Ursula-Cluster-Id` | refuse `wrong_cluster` |
//! | any | `404` | refuse `not_found` |
//! | any | another content type | refuse `stream_format` |
//! | none, replaced | `200`, closed or not | build, the replaced one trashed |
//! | trusted, same token, a snapshot past its ACK | `200`, closed or not | build, the generation trashed |
//! | trusted, same token | `200`, closed or not | catch up in place |
//! | trusted, other token | `200`, closed or not | build, the generation trashed |
//!
//! A reader generation behind the stream's latest snapshot builds from it rather than reading the
//! stream below it ([`behind_snapshot`]), which may be an import's claim.
//!
//! When the reader's first HEAD got no answer with nothing sent (the stream is unreachable),
//! [`decide_reader_unreachable`] opens a trusted reader generation as it is, and otherwise waits:
//! the open retries until its signal fires.
//!
//! A host's drain of a root (`OwnerMode::Drain`) decides by [`decide_drain`], after the same checks
//! of the HEAD. It only continues the claim the root's log holds, provisionally, behind the
//! pre-check (design: Pre-check), even when the stream answers: it never takes the stream over,
//! creates it or builds a generation, so it never claims above a claim an explicit open installed
//! before the pre-check read the tail.
//!
//! | local generation | HEAD | row |
//! | --- | --- | --- |
//! | any | no `Ursula-Protocol` | refuse `no_server_identity` |
//! | any | `Ursula-Protocol` of another release line | refuse `protocol_mismatch` |
//! | trusted | another `Ursula-Cluster-Id` | refuse `wrong_cluster` |
//! | any | `404` | refuse `not_found` |
//! | any | another content type | refuse `stream_format` |
//! | any | closed | refuse `stream_state` |
//! | trusted, with a CLAIM record | `200`, any token | continue, on the log's token |
//! | replaced | `200` | decline `replaced` |
//! | none, trusted without a CLAIM record | `200` | decline: nothing to continue |
//!
//! Another token than the log's is the pre-check's HEAD's to decide, so the drain has no history
//! rule of its own, and the stream's latest snapshot plays no part. When the drain's first HEAD
//! got no answer with nothing sent, [`decide_drain_unreachable`] continues by the unreachable row
//! and declines at once where an owner's open would wait. Every refusal and every decline keeps the
//! local files and sends nothing but the HEAD.

use ursula_headers::ClusterId;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;
use ursula_headers::ServerIdentity;
use ursula_headers::StreamHead;

use super::OpenError;
use super::StreamFormat;
use super::StreamState;
use crate::codec::frame::CONTENT_TYPE;
use crate::ids::Activation;
use crate::replicator::answer::Unacked;
use crate::replicator::answer::inherits;
use crate::replicator::answer::takeover_may_rekey;
use crate::salvage::Reason;

/// What the open knows of its trusted local generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalSummary {
    /// The cluster id the generation was built on.
    pub(crate) cluster_id: String,
    /// The token of the generation's latest segment.
    pub(crate) incarnation: Incarnation,
    /// The log's latest acknowledged offset.
    pub(crate) acked_offset: Offset,
    /// Whether the log holds a commit of the generation after its latest ACK.
    pub(crate) pending: Unacked,
    /// Whether the generation holds data ([`crate::materialize::holds_data`], design: Open).
    pub(crate) holds_data: bool,
    /// The log's latest CLAIM record: the claim a provisional open sends again once the stream
    /// answered and its pre-check found nothing foreign (design: Pre-check).
    pub(crate) claim: Option<(Epoch, Activation)>,
}

/// The local generation the open found: `L` the value the rows carry for a trusted one, `R` for a
/// replaced one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Local<L, R> {
    None,
    /// A generation whose db file is not its own, or whose log's first segment header is
    /// unreadable: salvaged as found (`replaced`), then as none.
    Replaced(R),
    Trusted(LocalSummary, L),
}

/// What the caller asked, and what the open did so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Asked {
    /// `recreate`: a missing stream is created again over a generation holding data.
    pub(crate) recreate: bool,
    /// This open created the stream.
    pub(crate) created: bool,
}

/// What a HEAD without a precondition answered (`200` or `404`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeadSeen<'a> {
    pub(crate) identity: Option<&'a ServerIdentity>,
    /// `None` on a `404`.
    pub(crate) stream: Option<&'a StreamHead>,
    pub(crate) content_type: Option<&'a str>,
}

/// What becomes of the local generation a build replaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Superseded<L, R> {
    /// There is none.
    Nothing,
    /// A trusted generation holding no data: trashed.
    Trash(L),
    /// A trusted generation holding data, salvaged for `why`.
    Salvage { why: Why, local: L },
    /// A replaced generation, salvaged as found (`replaced`).
    Replaced(R),
}

/// Why a build salvages a trusted generation: never `replaced`, which only a replaced one is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Why {
    HistoryChanged,
    Recreated,
}

impl From<Why> for Reason {
    fn from(why: Why) -> Self {
        match why {
            Why::HistoryChanged => Reason::HistoryChanged,
            Why::Recreated => Reason::Recreated,
        }
    }
}

/// What the open does, carrying what it does it with: the local generation `L` when a row uses
/// it, and the stream's identity and head when a row needs a stream. No row can pair with the
/// wrong local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenRow<L, R> {
    /// Create the stream, then HEAD again: the local generation stays as found, for that HEAD.
    Create { local: Local<L, R> },
    /// Build a generation from the stream, replacing the local one as `superseded` says.
    Build {
        superseded: Superseded<L, R>,
        cluster_id: ClusterId,
        head: StreamHead,
    },
    /// The generation has the stream's token: take over with it.
    Resume {
        local: L,
        cluster_id: ClusterId,
        head: StreamHead,
    },
    /// The generation has another token whose history inherits it: re-key from `from`, then take
    /// over (design: History).
    ReKey {
        local: L,
        from: Incarnation,
        cluster_id: ClusterId,
        head: StreamHead,
    },
    /// Refused: the local generation stays as it was found.
    Refuse {
        refused: Refused,
        local: Local<L, R>,
    },
}

/// Why the open refuses. Each keeps the local files and sends nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refused {
    /// The HEAD answer carries no `Ursula-Protocol` and `Ursula-Cluster-Id`.
    NoServerIdentity,
    /// The server speaks the release line `found`, not this extension's.
    ProtocolMismatch {
        found: u32,
    },
    WrongCluster {
        local: String,
        stream: ClusterId,
    },
    /// The stream is missing and the trusted generation holds data, or a reader opens it.
    NotFound,
    ContentType {
        found: Option<String>,
    },
    Closed,
}

impl From<Refused> for OpenError {
    fn from(refused: Refused) -> Self {
        match refused {
            Refused::NoServerIdentity => Self::NoServerIdentity,
            Refused::ProtocolMismatch { found } => Self::ProtocolMismatch { found },
            Refused::WrongCluster { local, stream } => Self::WrongCluster { local, stream },
            Refused::NotFound => Self::NotFound,
            Refused::ContentType { found } => {
                Self::StreamFormat(StreamFormat::ContentType { found })
            }
            Refused::Closed => Self::StreamState(StreamState::Closed),
        }
    }
}

/// Why a drain's open does not continue the root, though the stream answers as one it could
/// continue. Only a drain's open declines. It keeps the local files and sends nothing but the HEAD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Declined {
    /// The generation's db file is not its own, or its log's first segment header is unreadable:
    /// the next owner's open salvages it (`replaced`), and a drain builds nothing.
    #[error(
        "the generation's db file is not its own, or its log's first segment header is \
         unreadable: the next owner's open salvages it"
    )]
    Replaced,
    /// No trusted generation, or one whose log holds no CLAIM record: there is no claim to
    /// continue.
    #[error("the root holds no claim to continue")]
    NoClaim,
}

impl Declined {
    /// The token a drain reports: `replaced`, and `internal` for a root that holds no claim, which
    /// cannot hold commits either (a generation logs a commit only after its claim).
    pub(crate) fn kind(self) -> &'static str {
        match self {
            Self::Replaced => "replaced",
            Self::NoClaim => "internal",
        }
    }
}

/// What a drain's open does (design: Pre-check), carrying the trusted generation when it
/// continues it. A refused or declined one goes back as it was found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DrainRow<L, R> {
    /// Bind `local` now, provisionally, on its log's token: the log's latest claim `claim` goes
    /// out again once the pre-check found nothing foreign.
    Continue {
        local: L,
        claim: (Epoch, Activation),
    },
    Decline {
        declined: Declined,
        local: Local<L, R>,
    },
    Refuse {
        refused: Refused,
        local: Local<L, R>,
    },
}

/// The drain's rows (design: Pre-check), on the local generation the open found and the HEAD it
/// got: the checks of every HEAD, then only a trusted generation with a CLAIM record continues.
pub(crate) fn decide_drain<L, R>(local: Local<L, R>, head: HeadSeen<'_>) -> DrainRow<L, R> {
    match check_head(&local, head, OnClosed::Refuse) {
        Ok(Some(_)) => drain_rows(local),
        Ok(None) => DrainRow::Refuse {
            refused: Refused::NotFound,
            local,
        },
        Err(refused) => DrainRow::Refuse { refused, local },
    }
}

/// The drain's unreachable rows: a trusted generation with a CLAIM record continues, as by the
/// owner's unreachable row ([`decide_unreachable`]), and anything else declines at once, as it
/// would once the stream answered.
pub(crate) fn decide_drain_unreachable<L, R>(local: Local<L, R>) -> DrainRow<L, R> {
    drain_rows(local)
}

/// What a drain does with the root once nothing refused the stream.
fn drain_rows<L, R>(local: Local<L, R>) -> DrainRow<L, R> {
    match decide_unreachable(local) {
        UnreachableRow::Provisional { local, claim } => DrainRow::Continue { local, claim },
        UnreachableRow::Wait { local } => {
            let declined = match &local {
                Local::Replaced(_) => Declined::Replaced,
                Local::None | Local::Trusted(..) => Declined::NoClaim,
            };
            DrainRow::Decline { declined, local }
        }
    }
}

/// What the open does when its first HEAD got no answer and nothing went out: the stream is
/// unreachable (design: Open).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnreachableRow<L, R> {
    /// Open now on the trusted generation `local`, its claim deferred: once the stream answers
    /// and the pre-check finds nothing foreign, the log's latest claim `claim` goes out again.
    Provisional {
        local: L,
        claim: (Epoch, Activation),
    },
    /// Retry until the open's signal fires, the local generation as it was found: none, a
    /// replaced one (salvaged by the build that replaces it, once the stream answers), or a
    /// trusted one with no CLAIM record, which has no claim to continue.
    Wait { local: Local<L, R> },
}

/// The unreachable rows (design: Open), on the local generation the open found.
pub(crate) fn decide_unreachable<L, R>(local: Local<L, R>) -> UnreachableRow<L, R> {
    match local {
        Local::Trusted(
            LocalSummary {
                claim: Some(claim), ..
            },
            local,
        ) => UnreachableRow::Provisional { local, claim },
        local @ (Local::None | Local::Replaced(_) | Local::Trusted(..)) => {
            UnreachableRow::Wait { local }
        }
    }
}

/// Whether the open re-keys the trusted generation `summary` to the history `head` names: the
/// history inherits it and its log holds nothing after its latest ACK.
pub(crate) fn rekeys(summary: &LocalSummary, head: &StreamHead) -> bool {
    inherits(head, summary.incarnation, summary.acked_offset) && takeover_may_rekey(summary.pending)
}

/// Whether the stream's latest snapshot, as `head` names it, lies past the trusted generation's
/// latest ACK: a build starts at the snapshot, where catching up would read the stream below it.
pub(crate) fn behind_snapshot(summary: &LocalSummary, head: &StreamHead) -> bool {
    head.snapshot()
        .is_some_and(|snapshot| snapshot.offset > summary.acked_offset)
}

/// Why a HEAD's identity is not one this extension takes ([`check_identity`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdentityRefused {
    /// No `Ursula-Protocol` and `Ursula-Cluster-Id`: a server before 0.8, or a layer that answers
    /// the HEAD itself, such as a gateway's concealing `404`. A server upgrade would not help it.
    NoServerIdentity,
    /// `Ursula-Protocol` names the release line `found`, not [`ursula_headers::PROTOCOL_LINE`].
    ProtocolMismatch { found: u32 },
    /// Another `Ursula-Cluster-Id` than the generation's `local`.
    WrongCluster { local: String, stream: ClusterId },
}

impl From<IdentityRefused> for Refused {
    fn from(refused: IdentityRefused) -> Self {
        match refused {
            IdentityRefused::NoServerIdentity => Self::NoServerIdentity,
            IdentityRefused::ProtocolMismatch { found } => Self::ProtocolMismatch { found },
            IdentityRefused::WrongCluster { local, stream } => Self::WrongCluster { local, stream },
        }
    }
}

/// The identity a HEAD named, checked (design: Identity): present, of this extension's release
/// line ([`ursula_headers::PROTOCOL_LINE`]), and of the cluster id `ours` when a generation names
/// one. The one check of every HEAD: the open table, a build's HEADs, the import's, the
/// snapshotter's and the replicator's while bound.
pub(crate) fn check_identity<'a>(
    identity: Option<&'a ServerIdentity>,
    ours: Option<&str>,
) -> Result<&'a ServerIdentity, IdentityRefused> {
    let identity = identity.ok_or(IdentityRefused::NoServerIdentity)?;
    if identity.protocol != ursula_headers::PROTOCOL_LINE {
        return Err(IdentityRefused::ProtocolMismatch {
            found: identity.protocol,
        });
    }
    match ours {
        Some(ours) if ours != identity.cluster_id.as_str() => Err(IdentityRefused::WrongCluster {
            local: ours.to_owned(),
            stream: identity.cluster_id.clone(),
        }),
        Some(_) | None => Ok(identity),
    }
}

/// The release line after this extension's, which tests send as a newer server's: it moves with
/// [`ursula_headers::PROTOCOL_LINE`].
#[cfg(test)]
pub(crate) fn next_line() -> u32 {
    ursula_headers::PROTOCOL_LINE
        .checked_add(1)
        .expect("a release line below u32::MAX")
}

/// Whether an open takes a closed stream: an owner's cannot write to it, a reader's reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnClosed {
    Refuse,
    Read,
}

/// The checks of a HEAD every open makes, in the table's order, before the local generation
/// decides anything: the protocol and the trusted generation's cluster id, then a `404` (`None`),
/// then the content type, then a closed stream when `on_closed` refuses it. The owner's table and
/// the reader rows share them.
fn check_head<L, R>(
    local: &Local<L, R>,
    head: HeadSeen<'_>,
    on_closed: OnClosed,
) -> Result<Option<(ClusterId, StreamHead)>, Refused> {
    let ours = match local {
        Local::Trusted(summary, _) => Some(summary.cluster_id.as_str()),
        Local::None | Local::Replaced(_) => None,
    };
    let identity = check_identity(head.identity, ours)?;
    let Some(stream) = head.stream else {
        return Ok(None);
    };
    if head.content_type != Some(CONTENT_TYPE) {
        let found = head.content_type.map(str::to_owned);
        return Err(Refused::ContentType { found });
    }
    if stream.closed() && on_closed == OnClosed::Refuse {
        return Err(Refused::Closed);
    }
    Ok(Some((identity.cluster_id.clone(), *stream)))
}

/// What a reader's open does (design: Readers), carrying the trusted reader generation when a row
/// uses it. Every other local generation is dropped: a build replaces it, trashed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReaderRow<L> {
    /// Build a reader generation from the stream, the local one trashed.
    Build {
        cluster_id: ClusterId,
        head: StreamHead,
    },
    /// The reader generation has the stream's token: read the stream from its acknowledged offset
    /// to the tail `head` names into it.
    CatchUp {
        local: L,
        cluster_id: ClusterId,
        head: StreamHead,
    },
    Refuse(Refused),
}

/// The reader rows (design: Readers), on the reader generation the open found and the HEAD it got.
pub(crate) fn decide_reader<L, R>(local: Local<L, R>, head: HeadSeen<'_>) -> ReaderRow<L> {
    let (cluster_id, head) = match check_head(&local, head, OnClosed::Read) {
        Ok(Some(stream)) => stream,
        Ok(None) => return ReaderRow::Refuse(Refused::NotFound),
        Err(refused) => return ReaderRow::Refuse(refused),
    };
    match local {
        Local::Trusted(summary, local)
            if summary.incarnation == head.incarnation() && !behind_snapshot(&summary, &head) =>
        {
            ReaderRow::CatchUp {
                local,
                cluster_id,
                head,
            }
        }
        Local::None | Local::Replaced(_) | Local::Trusted(..) => {
            ReaderRow::Build { cluster_id, head }
        }
    }
}

/// What a reader's open does when its first HEAD got no answer and nothing went out (design:
/// Readers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReaderUnreachableRow<L, R> {
    /// Open the trusted reader generation `local` as it is: the stream as of its last open.
    Open { local: L },
    /// Retry until the open's signal fires, the local generation as it was found.
    Wait { local: Local<L, R> },
}

/// The unreachable reader rows (design: Readers), on the reader generation the open found.
pub(crate) fn decide_reader_unreachable<L, R>(local: Local<L, R>) -> ReaderUnreachableRow<L, R> {
    match local {
        Local::Trusted(_, local) => ReaderUnreachableRow::Open { local },
        local @ (Local::None | Local::Replaced(_)) => ReaderUnreachableRow::Wait { local },
    }
}

/// The open table (design: Open) for an owner's open, on the local generation it found, what was
/// asked, and the HEAD it got.
pub(crate) fn decide<L, R>(local: Local<L, R>, asked: Asked, head: HeadSeen<'_>) -> OpenRow<L, R> {
    let refuse = |refused, local| OpenRow::Refuse { refused, local };
    let (cluster_id, head) = match check_head(&local, head, OnClosed::Refuse) {
        Ok(Some(stream)) => stream,
        Ok(None) => {
            let not_found = matches!(
                &local,
                Local::Trusted(summary, _) if summary.holds_data && !asked.recreate
            );
            return if not_found {
                refuse(Refused::NotFound, local)
            } else {
                OpenRow::Create { local }
            };
        }
        Err(refused) => return refuse(refused, local),
    };
    let (summary, value) = match local {
        Local::None => {
            return OpenRow::Build {
                superseded: Superseded::Nothing,
                cluster_id,
                head,
            };
        }
        Local::Replaced(replaced) => {
            return OpenRow::Build {
                superseded: Superseded::Replaced(replaced),
                cluster_id,
                head,
            };
        }
        Local::Trusted(summary, value) => (summary, value),
    };
    if summary.incarnation == head.incarnation() {
        if !summary.holds_data && behind_snapshot(&summary, &head) {
            return OpenRow::Build {
                superseded: Superseded::Trash(value),
                cluster_id,
                head,
            };
        }
        return OpenRow::Resume {
            local: value,
            cluster_id,
            head,
        };
    }
    if rekeys(&summary, &head) {
        return OpenRow::ReKey {
            local: value,
            from: summary.incarnation,
            cluster_id,
            head,
        };
    }
    let superseded = if summary.holds_data {
        let why = if asked.created {
            Why::Recreated
        } else {
            Why::HistoryChanged
        };
        Superseded::Salvage { why, local: value }
    } else {
        Superseded::Trash(value)
    };
    OpenRow::Build {
        superseded,
        cluster_id,
        head,
    }
}

#[cfg(test)]
mod tests {
    use ursula_headers::ClusterId;
    use ursula_headers::Epoch;
    use ursula_headers::Incarnation;
    use ursula_headers::Inherited;
    use ursula_headers::Offset;
    use ursula_headers::ServerIdentity;
    use ursula_headers::SnapshotRef;
    use ursula_headers::StreamHead;

    use super::Asked;
    use super::Declined;
    use super::DrainRow;
    use super::HeadSeen;
    use super::Local;
    use super::LocalSummary;
    use super::OpenRow;
    use super::ReaderRow;
    use super::ReaderUnreachableRow;
    use super::Refused;
    use super::Superseded;
    use super::UnreachableRow;
    use super::Why;
    use super::decide;
    use super::decide_drain;
    use super::decide_drain_unreachable;
    use super::decide_reader;
    use super::decide_reader_unreachable;
    use super::decide_unreachable;
    use crate::codec::digest::SnapshotDigester;
    use crate::codec::frame::CONTENT_TYPE;
    use crate::ids::Activation;
    use crate::replicator::answer::Unacked;

    /// A local generation of the tests: a trusted one carries `"gen"`, a replaced one `"bad"`.
    type Found = Local<&'static str, &'static str>;
    type Row = OpenRow<&'static str, &'static str>;

    fn token(n: u64) -> Incarnation {
        Incarnation::try_from((1 << 52) | n).unwrap()
    }

    fn cluster(name: &str) -> ClusterId {
        ClusterId::new(name).unwrap()
    }

    fn identity(name: &str) -> ServerIdentity {
        ServerIdentity::current(cluster(name))
    }

    fn stream(t: u64, tail: u64, inherited: Option<(u64, u64)>) -> StreamHead {
        StreamHead::new(token(t), Offset::from_u64(tail), Offset::ZERO, 1 << 20)
            .unwrap()
            .with_inherited(inherited.map(|(previous, offset)| Inherited {
                previous_incarnation: token(previous),
                offset: Offset::from_u64(offset),
            }))
            .unwrap()
    }

    /// `head` naming a snapshot at `offset`.
    fn with_snapshot(head: StreamHead, offset: u64) -> StreamHead {
        let mut digester = SnapshotDigester::new(CONTENT_TYPE);
        digester.update(b"db");
        head.with_snapshot(Some(SnapshotRef {
            offset: Offset::from_u64(offset),
            digest: digester.finalize(),
        }))
        .unwrap()
    }

    /// A trusted generation of token `t` holding data, nothing pending, acknowledged up to
    /// `acked_offset`.
    fn local(t: u64, acked_offset: u64) -> Found {
        trusted(t, acked_offset, Unacked::Nothing, true)
    }

    fn trusted(t: u64, acked_offset: u64, pending: Unacked, holds_data: bool) -> Found {
        Local::Trusted(
            LocalSummary {
                cluster_id: "cluster-a".to_owned(),
                incarnation: token(t),
                acked_offset: Offset::from_u64(acked_offset),
                pending,
                holds_data,
                claim: Some((Epoch::try_from(3).unwrap(), Activation([3; 16]))),
            },
            "gen",
        )
    }

    const NONE: Found = Local::None;
    const REPLACED: Found = Local::Replaced("bad");

    fn seen<'a>(identity: &'a ServerIdentity, stream: Option<&'a StreamHead>) -> HeadSeen<'a> {
        HeadSeen {
            identity: Some(identity),
            stream,
            content_type: stream.map(|_| CONTENT_TYPE),
        }
    }

    fn ask() -> Asked {
        Asked::default()
    }

    const RECREATE: Asked = Asked {
        recreate: true,
        created: false,
    };

    /// The refusal of `local`, which the row hands back as found.
    fn refused(refused: Refused, local: Found) -> Row {
        OpenRow::Refuse { refused, local }
    }

    fn build(superseded: Superseded<&'static str, &'static str>, head: StreamHead) -> Row {
        OpenRow::Build {
            superseded,
            cluster_id: cluster("cluster-a"),
            head,
        }
    }

    fn salvaged(why: Why) -> Superseded<&'static str, &'static str> {
        Superseded::Salvage { why, local: "gen" }
    }

    /// The identity of cluster `name` on the release line `protocol`.
    fn line(name: &str, protocol: u32) -> ServerIdentity {
        ServerIdentity {
            protocol,
            cluster_id: cluster(name),
        }
    }

    // A server of another release line (0.8 to 0.10 send 2, a newer one its line) is refused
    // `protocol_mismatch`, and an answer without identity `no_server_identity`, whatever the local
    // generation, before its cluster is compared, by the owner's rows and the reader's.
    #[test]
    fn another_line_or_no_identity_is_refused_before_anything_else() {
        let head = stream(1, 0, None);
        let old = line("cluster-a", 2);
        let next = super::next_line();
        let newer = line("cluster-a", next);
        let elsewhere = line("cluster-b", next);
        for (identity, expected) in [
            (None, Refused::NoServerIdentity),
            (Some(&old), Refused::ProtocolMismatch { found: 2 }),
            (Some(&newer), Refused::ProtocolMismatch { found: next }),
            (Some(&elsewhere), Refused::ProtocolMismatch { found: next }),
        ] {
            let seen = HeadSeen {
                identity,
                stream: Some(&head),
                content_type: Some(CONTENT_TYPE),
            };
            for found in [local(1, 0), NONE, REPLACED] {
                assert_eq!(
                    decide(found.clone(), ask(), seen),
                    refused(expected.clone(), found.clone())
                );
                assert_eq!(
                    decide_reader(found, seen),
                    ReaderRow::Refuse(expected.clone())
                );
            }
        }
    }

    // A 404 without `Ursula-Protocol` (a gateway concealing a stream) is no missing stream: it
    // refuses `no_server_identity` whatever the local generation and `recreate`, never creates.
    #[test]
    fn a_404_without_identity_is_refused_not_missing() {
        let seen = HeadSeen {
            identity: None,
            stream: None,
            content_type: None,
        };
        let refusal = Refused::NoServerIdentity;
        for found in [local(1, 0), NONE, REPLACED] {
            for asked in [ask(), RECREATE] {
                assert_eq!(
                    decide(found.clone(), asked, seen),
                    refused(refusal.clone(), found.clone())
                );
            }
            assert_eq!(
                decide_reader(found, seen),
                ReaderRow::Refuse(refusal.clone())
            );
        }
    }

    #[test]
    fn another_cluster_refuses_a_trusted_generation_before_anything_else() {
        let id = identity("cluster-b");
        let head = stream(1, 0, None);
        let wrong = Refused::WrongCluster {
            local: "cluster-a".to_owned(),
            stream: cluster("cluster-b"),
        };
        assert_eq!(
            decide(local(1, 0), ask(), seen(&id, Some(&head))),
            refused(wrong.clone(), local(1, 0))
        );
        assert_eq!(
            decide(local(1, 0), RECREATE, seen(&id, None)),
            refused(wrong, local(1, 0))
        );
        // A fresh host has nothing to compare.
        assert_eq!(
            decide(NONE, ask(), seen(&id, Some(&head))),
            OpenRow::Build {
                superseded: Superseded::Nothing,
                cluster_id: cluster("cluster-b"),
                head
            }
        );
    }

    #[test]
    fn a_missing_stream_is_created_unless_a_trusted_generation_holds_data() {
        let id = identity("cluster-a");
        let none = seen(&id, None);
        for found in [NONE, REPLACED, trusted(1, 0, Unacked::Nothing, false)] {
            assert_eq!(decide(found.clone(), ask(), none), OpenRow::Create {
                local: found
            });
        }
        assert_eq!(
            decide(local(1, 0), ask(), none),
            refused(Refused::NotFound, local(1, 0))
        );
        assert_eq!(decide(local(1, 0), RECREATE, none), OpenRow::Create {
            local: local(1, 0)
        });
    }

    #[test]
    fn another_content_type_and_a_closed_stream_are_refused() {
        let id = identity("cluster-a");
        let head = stream(1, 0, None);
        let other = HeadSeen {
            content_type: Some("application/octet-stream"),
            ..seen(&id, Some(&head))
        };
        assert_eq!(
            decide(NONE, ask(), other),
            refused(
                Refused::ContentType {
                    found: Some("application/octet-stream".to_owned())
                },
                NONE
            )
        );
        let closed = head.with_closed(true);
        assert_eq!(
            decide(local(1, 0), ask(), seen(&id, Some(&closed))),
            refused(Refused::Closed, local(1, 0))
        );
    }

    #[test]
    fn no_generation_builds_and_the_same_token_resumes() {
        let id = identity("cluster-a");
        let head = stream(1, 40, None);
        assert_eq!(
            decide(NONE, ask(), seen(&id, Some(&head))),
            build(Superseded::Nothing, head)
        );
        assert_eq!(
            decide(local(1, 40), ask(), seen(&id, Some(&head))),
            OpenRow::Resume {
                local: "gen",
                cluster_id: cluster("cluster-a"),
                head
            }
        );
        // Pending commits resume under the same token.
        assert!(matches!(
            decide(
                trusted(1, 40, Unacked::Commits, true),
                ask(),
                seen(&id, Some(&head))
            ),
            OpenRow::Resume { .. }
        ));
    }

    // A generation of the stream's token holding no data, acknowledged below the stream's latest
    // snapshot (the claim of an import, say), is built over from the snapshot: it has nothing to
    // keep, and the stream below the snapshot may not be readable. One holding data, or one at or
    // past the snapshot, resumes.
    #[test]
    fn an_empty_generation_behind_the_snapshot_is_built_over_not_resumed() {
        let id = identity("cluster-a");
        let head = with_snapshot(stream(1, 80, None), 60);
        for acked in [0, 59] {
            assert_eq!(
                decide(
                    trusted(1, acked, Unacked::Nothing, false),
                    ask(),
                    seen(&id, Some(&head))
                ),
                build(Superseded::Trash("gen"), head)
            );
        }
        for found in [
            trusted(1, 60, Unacked::Nothing, false),
            trusted(1, 70, Unacked::Nothing, false),
            local(1, 0),
            trusted(1, 0, Unacked::Commits, true),
        ] {
            assert!(matches!(
                decide(found, ask(), seen(&id, Some(&head))),
                OpenRow::Resume { .. }
            ));
        }
    }

    #[test]
    fn a_replaced_generation_is_salvaged_and_built_over() {
        let id = identity("cluster-a");
        let head = stream(1, 40, None);
        assert_eq!(
            decide(REPLACED, ask(), seen(&id, Some(&head))),
            build(Superseded::Replaced("bad"), head)
        );
    }

    #[test]
    fn another_token_whose_history_inherits_the_acknowledged_offset_re_keys() {
        let id = identity("cluster-a");
        let head = stream(2, 50, Some((1, 40)));
        for acked in [40, 12] {
            assert_eq!(
                decide(local(1, acked), ask(), seen(&id, Some(&head))),
                OpenRow::ReKey {
                    local: "gen",
                    from: token(1),
                    cluster_id: cluster("cluster-a"),
                    head
                }
            );
        }
    }

    #[test]
    fn an_inheriting_history_salvages_a_generation_with_pending_commits() {
        let id = identity("cluster-a");
        let head = stream(2, 50, Some((1, 40)));
        assert_eq!(
            decide(
                trusted(1, 40, Unacked::Commits, true),
                ask(),
                seen(&id, Some(&head))
            ),
            build(salvaged(Why::HistoryChanged), head)
        );
    }

    #[test]
    fn another_token_that_does_not_inherit_salvages_or_trashes() {
        let id = identity("cluster-a");
        // The history lost bytes this generation acknowledged.
        let lossy = stream(2, 50, Some((1, 39)));
        assert_eq!(
            decide(local(1, 40), ask(), seen(&id, Some(&lossy))),
            build(salvaged(Why::HistoryChanged), lossy)
        );
        // It began from another token's history.
        let other = stream(3, 50, Some((2, 50)));
        assert_eq!(
            decide(local(1, 40), ask(), seen(&id, Some(&other))),
            build(salvaged(Why::HistoryChanged), other)
        );
        // It names no history it began from: a recreate, by this open or another.
        let recreated = stream(4, 0, None);
        assert_eq!(
            decide(local(1, 0), ask(), seen(&id, Some(&recreated))),
            build(salvaged(Why::HistoryChanged), recreated)
        );
        let created = Asked {
            recreate: true,
            created: true,
        };
        assert_eq!(
            decide(local(1, 0), created, seen(&id, Some(&recreated))),
            build(salvaged(Why::Recreated), recreated)
        );
        // A generation holding no data loses nothing.
        assert_eq!(
            decide(
                trusted(1, 0, Unacked::Nothing, false),
                ask(),
                seen(&id, Some(&recreated))
            ),
            build(Superseded::Trash("gen"), recreated)
        );
    }

    #[test]
    fn an_unreachable_stream_opens_a_trusted_generation_with_a_claim_provisionally() {
        assert_eq!(
            decide_unreachable(local(1, 40)),
            UnreachableRow::Provisional {
                local: "gen",
                claim: (Epoch::try_from(3).unwrap(), Activation([3; 16])),
            }
        );
        // Pending commits wait for the claim with the rest.
        assert!(matches!(
            decide_unreachable(trusted(1, 40, Unacked::Commits, true)),
            UnreachableRow::Provisional { .. }
        ));
    }

    #[test]
    fn an_unreachable_stream_waits_without_a_generation_a_claim_to_continue_or_with_a_replaced_one()
    {
        for found in [NONE, REPLACED] {
            assert_eq!(decide_unreachable(found.clone()), UnreachableRow::Wait {
                local: found
            });
        }
        let Local::Trusted(summary, value) = local(1, 40) else {
            unreachable!("local() is trusted");
        };
        let unclaimed: Found = Local::Trusted(
            LocalSummary {
                claim: None,
                ..summary
            },
            value,
        );
        assert_eq!(
            decide_unreachable(unclaimed.clone()),
            UnreachableRow::Wait { local: unclaimed }
        );
    }

    type ReadRow = ReaderRow<&'static str>;

    fn reader_build(head: StreamHead) -> ReadRow {
        ReaderRow::Build {
            cluster_id: cluster("cluster-a"),
            head,
        }
    }

    // A reader refuses what an owner refuses before any stream is read, and a missing stream
    // whatever it holds locally: it never creates one.
    #[test]
    fn a_reader_refuses_an_old_server_another_cluster_and_a_missing_stream() {
        let head = stream(1, 0, None);
        let no_identity = HeadSeen {
            identity: None,
            stream: Some(&head),
            content_type: Some(CONTENT_TYPE),
        };
        assert_eq!(
            decide_reader(local(1, 0), no_identity),
            ReaderRow::Refuse(Refused::NoServerIdentity)
        );
        let b = identity("cluster-b");
        assert_eq!(
            decide_reader(local(1, 0), seen(&b, Some(&head))),
            ReaderRow::Refuse(Refused::WrongCluster {
                local: "cluster-a".to_owned(),
                stream: cluster("cluster-b"),
            })
        );
        let a = identity("cluster-a");
        for found in [
            NONE,
            REPLACED,
            local(1, 0),
            trusted(1, 0, Unacked::Nothing, false),
        ] {
            assert_eq!(
                decide_reader(found, seen(&a, None)),
                ReaderRow::Refuse(Refused::NotFound)
            );
        }
        let other = HeadSeen {
            content_type: Some("text/plain"),
            ..seen(&a, Some(&head))
        };
        assert_eq!(
            decide_reader(NONE, other),
            ReaderRow::Refuse(Refused::ContentType {
                found: Some("text/plain".to_owned())
            })
        );
    }

    // The same token catches up in place. Anything else builds anew, an inheriting history too: a
    // reader generation holds nothing of its own to keep.
    #[test]
    fn a_reader_catches_up_the_same_token_and_builds_over_anything_else() {
        let id = identity("cluster-a");
        let head = stream(1, 40, None);
        assert_eq!(
            decide_reader(local(1, 12), seen(&id, Some(&head))),
            ReaderRow::CatchUp {
                local: "gen",
                cluster_id: cluster("cluster-a"),
                head
            }
        );
        for found in [NONE, REPLACED] {
            assert_eq!(
                decide_reader(found, seen(&id, Some(&head))),
                reader_build(head)
            );
        }
        let inheriting = stream(2, 50, Some((1, 40)));
        assert_eq!(
            decide_reader(local(1, 40), seen(&id, Some(&inheriting))),
            reader_build(inheriting)
        );
        let lossy = stream(2, 50, Some((1, 39)));
        assert_eq!(
            decide_reader(local(1, 40), seen(&id, Some(&lossy))),
            reader_build(lossy)
        );
    }

    // A reader generation acknowledged below the stream's latest snapshot builds from it rather
    // than catching up in place through the stream below it (an import's claim, say).
    #[test]
    fn a_reader_behind_the_snapshot_builds_from_it() {
        let id = identity("cluster-a");
        let head = with_snapshot(stream(1, 80, None), 60);
        for found in [local(1, 0), trusted(1, 59, Unacked::Nothing, false)] {
            assert_eq!(
                decide_reader(found, seen(&id, Some(&head))),
                reader_build(head)
            );
        }
        for acked in [60, 70] {
            assert_eq!(
                decide_reader(local(1, acked), seen(&id, Some(&head))),
                ReaderRow::CatchUp {
                    local: "gen",
                    cluster_id: cluster("cluster-a"),
                    head
                }
            );
        }
    }

    // A reader reads a closed stream, which an owner refuses: it writes nothing to it.
    #[test]
    fn a_reader_reads_a_closed_stream() {
        let id = identity("cluster-a");
        let closed = stream(1, 40, None).with_closed(true);
        assert_eq!(
            decide_reader(NONE, seen(&id, Some(&closed))),
            reader_build(closed)
        );
        assert_eq!(
            decide_reader(local(1, 12), seen(&id, Some(&closed))),
            ReaderRow::CatchUp {
                local: "gen",
                cluster_id: cluster("cluster-a"),
                head: closed
            }
        );
        assert_eq!(
            decide(NONE, ask(), seen(&id, Some(&closed))),
            refused(Refused::Closed, NONE)
        );
    }

    // A reader with no local generation has nothing to compare a cluster id with: it builds from
    // whatever deployment answers, as an owner does.
    #[test]
    fn a_reader_with_no_generation_builds_from_any_cluster() {
        let b = identity("cluster-b");
        let head = stream(1, 40, None);
        assert_eq!(
            decide_reader(NONE, seen(&b, Some(&head))),
            ReaderRow::Build {
                cluster_id: cluster("cluster-b"),
                head
            }
        );
    }

    #[test]
    fn an_unreachable_reader_opens_a_trusted_generation_and_waits_without_one() {
        assert_eq!(
            decide_reader_unreachable(local(1, 40)),
            ReaderUnreachableRow::Open { local: "gen" }
        );
        for found in [NONE, REPLACED] {
            assert_eq!(
                decide_reader_unreachable(found.clone()),
                ReaderUnreachableRow::Wait { local: found }
            );
        }
    }

    type Drain = DrainRow<&'static str, &'static str>;

    /// The claim `local()` and `trusted()` log.
    fn logged_claim() -> (Epoch, Activation) {
        (Epoch::try_from(3).unwrap(), Activation([3; 16]))
    }

    fn continues() -> Drain {
        DrainRow::Continue {
            local: "gen",
            claim: logged_claim(),
        }
    }

    /// `found` without a CLAIM record.
    fn unclaimed(found: Found) -> Found {
        let Local::Trusted(summary, value) = found else {
            unreachable!("a trusted generation");
        };
        Local::Trusted(
            LocalSummary {
                claim: None,
                ..summary
            },
            value,
        )
    }

    // A drain refuses what every open refuses before any stream is read, in the table's order,
    // and a missing stream whatever it holds locally: it never creates one. Each refusal hands the
    // local generation back as found.
    #[test]
    fn a_drain_refuses_an_old_server_another_cluster_a_missing_stream_and_its_state() {
        let head = stream(1, 40, None);
        let old = line("cluster-a", 2);
        for (identity, expected) in [
            (None, Refused::NoServerIdentity),
            (Some(&old), Refused::ProtocolMismatch { found: 2 }),
        ] {
            let seen = HeadSeen {
                identity,
                stream: Some(&head),
                content_type: Some(CONTENT_TYPE),
            };
            for found in [local(1, 0), NONE, REPLACED] {
                assert_eq!(decide_drain(found.clone(), seen), DrainRow::Refuse {
                    refused: expected.clone(),
                    local: found
                });
            }
        }
        let b = identity("cluster-b");
        assert_eq!(
            decide_drain(local(1, 0), seen(&b, Some(&head))),
            DrainRow::Refuse {
                refused: Refused::WrongCluster {
                    local: "cluster-a".to_owned(),
                    stream: cluster("cluster-b"),
                },
                local: local(1, 0)
            }
        );
        let a = identity("cluster-a");
        for found in [
            NONE,
            REPLACED,
            local(1, 0),
            trusted(1, 0, Unacked::Commits, true),
            trusted(1, 0, Unacked::Nothing, false),
        ] {
            assert_eq!(
                decide_drain(found.clone(), seen(&a, None)),
                DrainRow::Refuse {
                    refused: Refused::NotFound,
                    local: found
                }
            );
        }
        let other = HeadSeen {
            content_type: Some("text/plain"),
            ..seen(&a, Some(&head))
        };
        assert_eq!(decide_drain(local(1, 0), other), DrainRow::Refuse {
            refused: Refused::ContentType {
                found: Some("text/plain".to_owned())
            },
            local: local(1, 0)
        });
        let closed = head.with_closed(true);
        assert_eq!(
            decide_drain(local(1, 0), seen(&a, Some(&closed))),
            DrainRow::Refuse {
                refused: Refused::Closed,
                local: local(1, 0)
            }
        );
    }

    // A trusted generation with a CLAIM record continues its claim whatever the stream's token (the
    // pre-check's HEAD decides another one) and its latest snapshot, with commits pending or not:
    // a drain never builds, re-keys or salvages at its open.
    #[test]
    fn a_drain_continues_a_trusted_generation_with_a_claim_whatever_the_token() {
        let id = identity("cluster-a");
        for head in [
            stream(1, 40, None),
            with_snapshot(stream(1, 80, None), 60),
            stream(2, 50, Some((1, 40))),
            stream(2, 50, Some((1, 39))),
            stream(4, 0, None),
        ] {
            for found in [
                local(1, 40),
                trusted(1, 40, Unacked::Commits, true),
                trusted(1, 0, Unacked::Nothing, false),
            ] {
                assert_eq!(
                    decide_drain(found, seen(&id, Some(&head))),
                    continues(),
                    "{head:?}"
                );
            }
        }
    }

    // What a drain cannot continue it declines, with nothing sent but the HEAD: a replaced
    // generation (the next owner's open salvages it), none, or a trusted one with no CLAIM record.
    #[test]
    fn a_drain_declines_a_replaced_generation_and_one_without_a_claim() {
        let id = identity("cluster-a");
        let head = stream(1, 40, None);
        assert_eq!(
            decide_drain(REPLACED, seen(&id, Some(&head))),
            DrainRow::Decline {
                declined: Declined::Replaced,
                local: REPLACED
            }
        );
        for found in [NONE, unclaimed(local(1, 40))] {
            assert_eq!(
                decide_drain(found.clone(), seen(&id, Some(&head))),
                DrainRow::Decline {
                    declined: Declined::NoClaim,
                    local: found
                }
            );
        }
        assert_eq!(Declined::Replaced.kind(), "replaced");
        assert_eq!(Declined::NoClaim.kind(), "internal");
    }

    // Unreachable, a drain continues by the owner's provisional row, and declines at once where an
    // owner's open would wait for the stream.
    #[test]
    fn an_unreachable_drain_continues_a_claim_and_declines_anything_else_at_once() {
        assert_eq!(decide_drain_unreachable(local(1, 40)), continues());
        assert_eq!(
            decide_drain_unreachable(trusted(1, 40, Unacked::Commits, true)),
            continues()
        );
        assert_eq!(decide_drain_unreachable(REPLACED), DrainRow::Decline {
            declined: Declined::Replaced,
            local: REPLACED
        });
        for found in [NONE, unclaimed(local(1, 40))] {
            assert_eq!(decide_drain_unreachable(found.clone()), DrainRow::Decline {
                declined: Declined::NoClaim,
                local: found
            });
        }
    }
}
