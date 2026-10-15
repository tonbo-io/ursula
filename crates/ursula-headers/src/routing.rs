//! Routing answers, sent before the request body is read (extensions
//! Section 5.7, design Section 6.1): a node that does not lead the stream's
//! Raft group redirects to the one that does, or refuses with `503` when it
//! knows none.
//!
//! The redirect's `Location` and the `503`'s `Retry-After` are standard
//! HTTP headers that the caller handles; this module types Ursula's own.

use std::fmt;

use crate::error::HeaderError;
use crate::fields;
use crate::names;

/// The id of a server node (`x-ursula-raft-leader-id`). Every `u64` is an
/// id; ids are compared for equality only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u64);

impl NodeId {
    pub const fn from_u64(id: u64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The Raft group that owns a stream (`x-ursula-raft-group`). Every `u32`
/// is a group; groups are compared for equality only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RaftGroup(u32);

impl RaftGroup {
    pub const fn from_u32(group: u32) -> Self {
        Self(group)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    fn parse(name: &'static str, text: &str) -> Result<Self, HeaderError> {
        let value = crate::value::parse_decimal(name, text)?;
        u32::try_from(value)
            .map(Self)
            .map_err(|_overflow| HeaderError::OutOfRange { name, value })
    }
}

impl fmt::Display for RaftGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A Raft term (`x-ursula-raft-term`). Terms order: a router keeps the hint
/// of the highest term it saw for a group and discards older ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RaftTerm(u64);

impl RaftTerm {
    pub const fn from_u64(term: u64) -> Self {
        Self(term)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RaftTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A routing answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingAnswer {
    /// `307 Temporary Redirect`: `leader` leads `group` in `term`; the
    /// `Location` names the same request target on it.
    NotLeader {
        leader: NodeId,
        group: RaftGroup,
        term: RaftTerm,
    },
    /// `503 Service Unavailable` with `Retry-After`: no leader of `group` is
    /// known.
    NoLeader { group: RaftGroup },
}

impl RoutingAnswer {
    pub fn status(&self) -> u16 {
        match self {
            Self::NotLeader { .. } => 307,
            Self::NoLeader { .. } => 503,
        }
    }

    /// The group the answer is about.
    pub fn group(&self) -> RaftGroup {
        match self {
            Self::NotLeader { group, .. } | Self::NoLeader { group } => *group,
        }
    }

    /// Emits Ursula's routing headers, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        match self {
            Self::NotLeader {
                leader,
                group,
                term,
            } => {
                emit(names::X_URSULA_RAFT_LEADER_ID, &leader.to_string());
                emit(names::X_URSULA_RAFT_GROUP, &group.to_string());
                emit(names::X_URSULA_RAFT_TERM, &term.to_string());
            }
            Self::NoLeader { group } => emit(names::X_URSULA_RAFT_GROUP, &group.to_string()),
        }
    }

    /// Parses a routing answer from its status and headers. `Ok(None)` for
    /// any other status, for a `307` without Ursula's routing headers (a
    /// redirect of another layer), and for a `503` without
    /// `x-ursula-raft-group` (pressure, not routing). A `307` with some but
    /// not all of the three headers is [`HeaderError::Missing`].
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        if !matches!(status, 307 | 503) {
            return Ok(None);
        }
        let [leader, group, term] = fields::collect(headers, [
            names::X_URSULA_RAFT_LEADER_ID,
            names::X_URSULA_RAFT_GROUP,
            names::X_URSULA_RAFT_TERM,
        ])?;
        let group = fields::parsed(names::X_URSULA_RAFT_GROUP, group, RaftGroup::parse)?;
        if status == 503 {
            return Ok(group.map(|group| Self::NoLeader { group }));
        }
        let leader = fields::decimal(names::X_URSULA_RAFT_LEADER_ID, leader)?.map(NodeId);
        let term = fields::decimal(names::X_URSULA_RAFT_TERM, term)?.map(RaftTerm);
        match (leader, group, term) {
            (None, None, None) => Ok(None),
            (Some(leader), Some(group), Some(term)) => Ok(Some(Self::NotLeader {
                leader,
                group,
                term,
            })),
            (None, ..) => Err(HeaderError::Missing {
                name: names::X_URSULA_RAFT_LEADER_ID,
            }),
            (_, None, _) => Err(HeaderError::Missing {
                name: names::X_URSULA_RAFT_GROUP,
            }),
            (.., None) => Err(HeaderError::Missing {
                name: names::X_URSULA_RAFT_TERM,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::NodeId;
    use super::RaftGroup;
    use super::RaftTerm;
    use super::RoutingAnswer;
    use crate::error::HeaderError;

    fn parse(status: u16, pairs: &[(&str, &str)]) -> Result<Option<RoutingAnswer>, HeaderError> {
        RoutingAnswer::parse(
            status,
            pairs.iter().map(|(name, value)| (*name, value.as_bytes())),
        )
    }

    #[test]
    fn routing_answers_round_trip() {
        for answer in [
            RoutingAnswer::NotLeader {
                leader: NodeId::from_u64(3),
                group: RaftGroup::from_u32(17),
                term: RaftTerm::from_u64(42),
            },
            RoutingAnswer::NotLeader {
                leader: NodeId::from_u64(u64::MAX),
                group: RaftGroup::from_u32(u32::MAX),
                term: RaftTerm::from_u64(u64::MAX),
            },
            RoutingAnswer::NoLeader {
                group: RaftGroup::from_u32(0),
            },
        ] {
            let mut pairs = Vec::new();
            answer.render(|name, value| pairs.push((name, value.to_owned())));
            let parsed = RoutingAnswer::parse(
                answer.status(),
                pairs.iter().map(|(name, value)| (*name, value.as_bytes())),
            );
            assert_eq!(parsed, Ok(Some(answer)), "{pairs:?}");
        }
    }

    #[test]
    fn the_redirect_keeps_the_leader_id_header_of_0_7() {
        let mut pairs = Vec::new();
        RoutingAnswer::NotLeader {
            leader: NodeId::from_u64(2),
            group: RaftGroup::from_u32(5),
            term: RaftTerm::from_u64(9),
        }
        .render(|name, value| pairs.push((name, value.to_owned())));
        assert_eq!(pairs, [
            ("x-ursula-raft-leader-id", "2".to_owned()),
            ("x-ursula-raft-group", "5".to_owned()),
            ("x-ursula-raft-term", "9".to_owned()),
        ]);
    }

    #[test]
    fn other_answers_are_not_routing_answers() {
        assert_eq!(parse(307, &[("location", "http://n2/s")]), Ok(None));
        assert_eq!(parse(503, &[("retry-after", "1")]), Ok(None));
        assert_eq!(parse(200, &[("x-ursula-raft-group", "x")]), Ok(None));
        // A 503 types only the group.
        assert_eq!(
            parse(503, &[
                ("x-ursula-raft-group", "4"),
                ("x-ursula-raft-term", "bad")
            ]),
            Ok(Some(RoutingAnswer::NoLeader {
                group: RaftGroup::from_u32(4)
            }))
        );
        assert!(RaftTerm::from_u64(2) > RaftTerm::from_u64(1));
    }

    #[test]
    fn malformed_routing_answers_are_typed() {
        for (status, pairs, expected) in [
            (
                307,
                &[("x-ursula-raft-leader-id", "2")][..],
                HeaderError::Missing {
                    name: "x-ursula-raft-group",
                },
            ),
            (
                307,
                &[
                    ("x-ursula-raft-leader-id", "2"),
                    ("x-ursula-raft-group", "1"),
                ][..],
                HeaderError::Missing {
                    name: "x-ursula-raft-term",
                },
            ),
            (
                307,
                &[("x-ursula-raft-group", "1"), ("x-ursula-raft-term", "1")][..],
                HeaderError::Missing {
                    name: "x-ursula-raft-leader-id",
                },
            ),
            (
                307,
                &[
                    ("x-ursula-raft-leader-id", "02"),
                    ("x-ursula-raft-group", "1"),
                    ("x-ursula-raft-term", "1"),
                ][..],
                HeaderError::NonCanonicalInteger {
                    name: "x-ursula-raft-leader-id",
                },
            ),
            (
                503,
                &[("x-ursula-raft-group", "4294967296")][..],
                HeaderError::OutOfRange {
                    name: "x-ursula-raft-group",
                    value: 4_294_967_296,
                },
            ),
            (
                503,
                &[("x-ursula-raft-group", "-1")][..],
                HeaderError::InvalidInteger {
                    name: "x-ursula-raft-group",
                },
            ),
        ] {
            assert_eq!(parse(status, pairs), Err(expected), "{pairs:?}");
        }
    }
}
