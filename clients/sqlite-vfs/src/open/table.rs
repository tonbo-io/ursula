//! The open table of design §5.5 as one pure function: what the open does with the local
//! generation it found and the HEAD it got. [`decide`] does no I/O; the open carries out its
//! [`OpenRow`].
//!
//! | local generation | HEAD | row |
//! | --- | --- | --- |
//! | any | no `Ursula-Protocol: 2` | refuse `server_too_old` |
//! | trusted | another `Ursula-Cluster-Id` | refuse `wrong_cluster` |
//! | none, replaced | `404` | create |
//! | trusted, holding no data | `404` | create |
//! | trusted, holding data | `404` | refuse `not_found`, or create with `recreate` |
//! | any | another content type | refuse `stream_format` |
//! | any | closed | refuse `stream_state` |
//! | none | `200` | build |
//! | replaced | `200` | build, the generation salvaged (`replaced`) |
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
//! When the open's first HEAD got no answer with nothing sent (the stream is unreachable, design
//! §5.5), [`decide_unreachable`] decides instead:
//!
//! | local generation | row |
//! | --- | --- |
//! | trusted, with a CLAIM record | open provisionally, the claim deferred behind a pre-check |
//! | trusted, without one | wait: retry until the open's signal fires |
//! | none | wait |
//!
//! A reader's open (design D6) decides by [`decide_reader`] instead, after the same checks of the
//! HEAD ([`check_head`]). A reader generation holds no commit of its own, only what it read, so it
//! is never salvaged and never re-keyed: anything the stream no longer extends is built anew. A
//! trusted reader generation of another cluster is refused as an owner's is (the stream it reads
//! is not the one it caught up with), and a reader with none builds, having nothing to compare. A
//! closed stream is read like any other: a reader writes nothing to it.
//!
//! | reader generation | HEAD | row |
//! | --- | --- | --- |
//! | any | no `Ursula-Protocol: 2` | refuse `server_too_old` |
//! | trusted | another `Ursula-Cluster-Id` | refuse `wrong_cluster` |
//! | any | `404` | refuse `not_found` |
//! | any | another content type | refuse `stream_format` |
//! | none, replaced | `200`, closed or not | build, the replaced one trashed |
//! | trusted, same token | `200`, closed or not | catch up in place |
//! | trusted, other token | `200`, closed or not | build, the generation trashed |
//!
//! When the reader's first HEAD got no answer with nothing sent (the stream is unreachable),
//! [`decide_reader_unreachable`] opens a trusted reader generation as it is, and otherwise waits:
//! the open retries until its signal fires.

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
    /// Whether the generation holds data ([`crate::materialize::holds_data`], design D2).
    pub(crate) holds_data: bool,
    /// The log's latest CLAIM record: the claim a provisional open sends again once the stream
    /// answered and its pre-check found nothing foreign (design §7).
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
    /// over (design §6.2).
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
    ServerTooOld {
        protocol: Option<u32>,
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
            Refused::ServerTooOld { protocol } => Self::ServerTooOld { protocol },
            Refused::WrongCluster { local, stream } => Self::WrongCluster { local, stream },
            Refused::NotFound => Self::NotFound,
            Refused::ContentType { found } => {
                Self::StreamFormat(StreamFormat::ContentType { found })
            }
            Refused::Closed => Self::StreamState(StreamState::Closed),
        }
    }
}

/// What the open does when its first HEAD got no answer and nothing went out: the stream is
/// unreachable (design §5.5).
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

/// The unreachable rows of design §5.5, on the local generation the open found.
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

/// The identity a HEAD named, checked (design §6.3): protocol 2, and the cluster id `ours` when a
/// generation names one. The open table and a build's HEADs share it.
pub(crate) fn check_identity<'a>(
    identity: Option<&'a ServerIdentity>,
    ours: Option<&str>,
) -> Result<&'a ServerIdentity, Refused> {
    let identity = match identity {
        Some(identity) if identity.protocol == ursula_headers::PROTOCOL_VERSION => identity,
        other => {
            let protocol = other.map(|i| i.protocol);
            return Err(Refused::ServerTooOld { protocol });
        }
    };
    match ours {
        Some(ours) if ours != identity.cluster_id.as_str() => Err(Refused::WrongCluster {
            local: ours.to_owned(),
            stream: identity.cluster_id.clone(),
        }),
        Some(_) | None => Ok(identity),
    }
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

/// What a reader's open does (design D6), carrying the trusted reader generation when a row uses
/// it. Every other local generation is dropped: a build replaces it, trashed.
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

/// The reader rows of design D6, on the reader generation the open found and the HEAD it got.
pub(crate) fn decide_reader<L, R>(local: Local<L, R>, head: HeadSeen<'_>) -> ReaderRow<L> {
    let (cluster_id, head) = match check_head(&local, head, OnClosed::Read) {
        Ok(Some(stream)) => stream,
        Ok(None) => return ReaderRow::Refuse(Refused::NotFound),
        Err(refused) => return ReaderRow::Refuse(refused),
    };
    match local {
        Local::Trusted(summary, local) if summary.incarnation == head.incarnation() => {
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

/// What a reader's open does when its first HEAD got no answer and nothing went out (design D6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReaderUnreachableRow<L, R> {
    /// Open the trusted reader generation `local` as it is: the stream as of its last open.
    Open { local: L },
    /// Retry until the open's signal fires, the local generation as it was found.
    Wait { local: Local<L, R> },
}

/// The unreachable reader rows of design D6, on the reader generation the open found.
pub(crate) fn decide_reader_unreachable<L, R>(local: Local<L, R>) -> ReaderUnreachableRow<L, R> {
    match local {
        Local::Trusted(_, local) => ReaderUnreachableRow::Open { local },
        local @ (Local::None | Local::Replaced(_)) => ReaderUnreachableRow::Wait { local },
    }
}

/// The table of design §5.5 for an owner's open, on the local generation it found, what was
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
    use ursula_headers::StreamHead;

    use super::Asked;
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
    use super::decide_reader;
    use super::decide_reader_unreachable;
    use super::decide_unreachable;
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

    #[test]
    fn a_server_without_protocol_2_is_too_old() {
        let head = stream(1, 0, None);
        let seen = HeadSeen {
            identity: None,
            stream: Some(&head),
            content_type: Some(CONTENT_TYPE),
        };
        let too_old = Refused::ServerTooOld { protocol: None };
        for found in [local(1, 0), NONE, REPLACED] {
            assert_eq!(
                decide(found.clone(), ask(), seen),
                refused(too_old.clone(), found)
            );
        }
    }

    // A 404 without `Ursula-Protocol` (a gateway concealing a stream) is no missing stream: it
    // refuses as too old whatever the local generation and `recreate`, never creates.
    #[test]
    fn a_404_without_protocol_2_is_too_old_not_missing() {
        let seen = HeadSeen {
            identity: None,
            stream: None,
            content_type: None,
        };
        let too_old = Refused::ServerTooOld { protocol: None };
        for found in [local(1, 0), NONE, REPLACED] {
            for asked in [ask(), RECREATE] {
                assert_eq!(
                    decide(found.clone(), asked, seen),
                    refused(too_old.clone(), found.clone())
                );
            }
            assert_eq!(
                decide_reader(found, seen),
                ReaderRow::Refuse(too_old.clone())
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
            ReaderRow::Refuse(Refused::ServerTooOld { protocol: None })
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
}
