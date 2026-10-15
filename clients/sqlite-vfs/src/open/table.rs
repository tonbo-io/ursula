//! The open table of design §5.5 as one pure function: what the open does with the local
//! generation it materialized and the HEAD it got. [`decide`] does no I/O; the open carries out its
//! [`OpenRow`].
//!
//! | local generation | HEAD | row |
//! | --- | --- | --- |
//! | any | no `Ursula-Protocol: 2` | refuse `server_too_old` |
//! | trusted | another `Ursula-Cluster-Id` | refuse `wrong_cluster` |
//! | none | `404` | create |
//! | trusted | `404` | refuse `not_found` |
//! | any | another content type | refuse `stream_format` |
//! | any | closed | refuse `stream_state` |
//! | none | `200` | build |
//! | trusted, same token | `200` | resume |
//! | trusted, other token, inheriting, nothing pending | `200` | re-key ([`inherits`]) |
//! | trusted, other token, otherwise | `200` | refuse `stream_state` (salvage comes later in M2) |
//!
//! Rows match in order, so a refusal sends nothing and moves no file.

use ursula_headers::ClusterId;
use ursula_headers::Incarnation;
use ursula_headers::Offset;
use ursula_headers::ServerIdentity;
use ursula_headers::StreamHead;

use super::OpenError;
use super::StreamFormat;
use super::StreamState;
use crate::codec::frame::CONTENT_TYPE;
use crate::replicator::answer::Unacked;
use crate::replicator::answer::inherits;
use crate::replicator::answer::takeover_may_rekey;

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
}

/// What a HEAD without a precondition answered (`200` or `404`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeadSeen<'a> {
    pub(crate) identity: Option<&'a ServerIdentity>,
    /// `None` on a `404`.
    pub(crate) stream: Option<&'a StreamHead>,
    pub(crate) content_type: Option<&'a str>,
}

/// What the open does, carrying what it does it with: the local generation `L` when a row uses
/// it, and the stream's identity and head when a row needs a stream. No row can pair with the
/// wrong local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenRow<L> {
    /// No stream and no local generation: create it, then HEAD again.
    Create,
    /// No local generation: build one from the stream.
    Build {
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
    /// Refused: the local generation, if any, stays as it was.
    Refuse { refused: Refused, local: Option<L> },
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
    /// The stream is missing and the generation is trusted.
    NotFound,
    ContentType {
        found: Option<String>,
    },
    Closed,
    /// Another token whose history does not inherit the generation, or one that does while the
    /// log holds commits after its latest ACK.
    TokenChanged {
        local: Incarnation,
        stream: Incarnation,
    },
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
            Refused::TokenChanged { local, stream } => {
                Self::StreamState(StreamState::TokenChanged { local, stream })
            }
        }
    }
}

/// The table of design §5.5 for an owner's open, on the trusted local generation `local` (its
/// summary and the value the rows carry) and the HEAD the open got.
pub(crate) fn decide<L>(local: Option<(LocalSummary, L)>, head: HeadSeen<'_>) -> OpenRow<L> {
    let refuse = |refused, local: Option<(LocalSummary, L)>| OpenRow::Refuse {
        refused,
        local: local.map(|(_, l)| l),
    };
    let identity = match head.identity {
        Some(identity) if identity.protocol == ursula_headers::PROTOCOL_VERSION => identity,
        other => {
            let protocol = other.map(|i| i.protocol);
            return refuse(Refused::ServerTooOld { protocol }, local);
        }
    };
    if let Some((summary, _)) = &local
        && summary.cluster_id != identity.cluster_id.as_str()
    {
        let refused = Refused::WrongCluster {
            local: summary.cluster_id.clone(),
            stream: identity.cluster_id.clone(),
        };
        return refuse(refused, local);
    }
    let Some(stream) = head.stream else {
        return match local {
            Some(_) => refuse(Refused::NotFound, local),
            None => OpenRow::Create,
        };
    };
    if head.content_type != Some(CONTENT_TYPE) {
        let found = head.content_type.map(str::to_owned);
        return refuse(Refused::ContentType { found }, local);
    }
    if stream.closed() {
        return refuse(Refused::Closed, local);
    }
    let cluster_id = identity.cluster_id.clone();
    let head = *stream;
    let Some((summary, value)) = local else {
        return OpenRow::Build { cluster_id, head };
    };
    if summary.incarnation == head.incarnation() {
        return OpenRow::Resume {
            local: value,
            cluster_id,
            head,
        };
    }
    if inherits(&head, summary.incarnation, summary.acked_offset)
        && takeover_may_rekey(summary.pending)
    {
        return OpenRow::ReKey {
            local: value,
            from: summary.incarnation,
            cluster_id,
            head,
        };
    }
    OpenRow::Refuse {
        refused: Refused::TokenChanged {
            local: summary.incarnation,
            stream: head.incarnation(),
        },
        local: Some(value),
    }
}

#[cfg(test)]
mod tests {
    use ursula_headers::ClusterId;
    use ursula_headers::Incarnation;
    use ursula_headers::Inherited;
    use ursula_headers::Offset;
    use ursula_headers::ServerIdentity;
    use ursula_headers::StreamHead;

    use super::HeadSeen;
    use super::LocalSummary;
    use super::OpenRow;
    use super::Refused;
    use super::decide;
    use crate::codec::frame::CONTENT_TYPE;
    use crate::replicator::answer::Unacked;

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

    /// A trusted generation of token `t` with nothing pending, acknowledged up to
    /// `acked_offset`; the rows carry `"gen"`.
    fn local(t: u64, acked_offset: u64) -> Option<(LocalSummary, &'static str)> {
        with_pending(t, acked_offset, Unacked::Nothing)
    }

    fn with_pending(
        t: u64,
        acked_offset: u64,
        pending: Unacked,
    ) -> Option<(LocalSummary, &'static str)> {
        Some((
            LocalSummary {
                cluster_id: "cluster-a".to_owned(),
                incarnation: token(t),
                acked_offset: Offset::from_u64(acked_offset),
                pending,
            },
            "gen",
        ))
    }

    fn seen<'a>(identity: &'a ServerIdentity, stream: Option<&'a StreamHead>) -> HeadSeen<'a> {
        HeadSeen {
            identity: Some(identity),
            stream,
            content_type: stream.map(|_| CONTENT_TYPE),
        }
    }

    fn refused(refused: Refused, local: Option<&'static str>) -> OpenRow<&'static str> {
        OpenRow::Refuse { refused, local }
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
        assert_eq!(
            decide(local(1, 0), seen),
            refused(too_old.clone(), Some("gen"))
        );
        assert_eq!(decide::<&str>(None, seen), refused(too_old, None));
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
            decide(local(1, 0), seen(&id, Some(&head))),
            refused(wrong.clone(), Some("gen"))
        );
        assert_eq!(
            decide(local(1, 0), seen(&id, None)),
            refused(wrong, Some("gen"))
        );
        // A fresh host has nothing to compare.
        assert_eq!(
            decide::<&str>(None, seen(&id, Some(&head))),
            OpenRow::Build {
                cluster_id: cluster("cluster-b"),
                head
            }
        );
    }

    #[test]
    fn a_missing_stream_is_created_without_a_generation_and_refused_with_one() {
        let id = identity("cluster-a");
        assert_eq!(decide::<&str>(None, seen(&id, None)), OpenRow::Create);
        assert_eq!(
            decide(local(1, 0), seen(&id, None)),
            refused(Refused::NotFound, Some("gen"))
        );
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
            decide::<&str>(None, other),
            refused(
                Refused::ContentType {
                    found: Some("application/octet-stream".to_owned())
                },
                None
            )
        );
        let closed = head.with_closed(true);
        assert_eq!(
            decide(local(1, 0), seen(&id, Some(&closed))),
            refused(Refused::Closed, Some("gen"))
        );
    }

    #[test]
    fn no_generation_builds_and_the_same_token_resumes() {
        let id = identity("cluster-a");
        let head = stream(1, 40, None);
        assert_eq!(
            decide::<&str>(None, seen(&id, Some(&head))),
            OpenRow::Build {
                cluster_id: cluster("cluster-a"),
                head
            }
        );
        assert_eq!(
            decide(local(1, 40), seen(&id, Some(&head))),
            OpenRow::Resume {
                local: "gen",
                cluster_id: cluster("cluster-a"),
                head
            }
        );
        // Pending commits resume under the same token.
        assert!(matches!(
            decide(
                with_pending(1, 40, Unacked::Commits),
                seen(&id, Some(&head))
            ),
            OpenRow::Resume { .. }
        ));
    }

    #[test]
    fn another_token_whose_history_inherits_the_acknowledged_offset_re_keys() {
        let id = identity("cluster-a");
        let head = stream(2, 50, Some((1, 40)));
        for acked in [40, 12] {
            assert_eq!(
                decide(local(1, acked), seen(&id, Some(&head))),
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
    fn an_inheriting_history_is_refused_while_the_log_holds_pending_commits() {
        let id = identity("cluster-a");
        let head = stream(2, 50, Some((1, 40)));
        assert_eq!(
            decide(
                with_pending(1, 40, Unacked::Commits),
                seen(&id, Some(&head))
            ),
            refused(
                Refused::TokenChanged {
                    local: token(1),
                    stream: token(2),
                },
                Some("gen")
            )
        );
    }

    #[test]
    fn another_token_that_does_not_inherit_is_refused() {
        let id = identity("cluster-a");
        let changed = |stream: u64| {
            refused(
                Refused::TokenChanged {
                    local: token(1),
                    stream: token(stream),
                },
                Some("gen"),
            )
        };
        // The history lost bytes this generation acknowledged.
        let lossy = stream(2, 50, Some((1, 39)));
        assert_eq!(decide(local(1, 40), seen(&id, Some(&lossy))), changed(2));
        // It began from another token's history.
        let other = stream(3, 50, Some((2, 50)));
        assert_eq!(decide(local(1, 40), seen(&id, Some(&other))), changed(3));
        // It names no history it began from (a recreate).
        let recreated = stream(4, 0, None);
        assert_eq!(decide(local(1, 0), seen(&id, Some(&recreated))), changed(4));
    }
}
