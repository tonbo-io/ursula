//! The usage gate: the narrowly scoped rolling-upgrade gate of 0.10.2
//! within format epoch 6 (`ursula_stream::FORMAT_EPOCH`'s policy).
//!
//! 0.10.2 adds one replicated command, `StreamCommand::AckStreamUsage`,
//! and the snapshot frames of a started usage ledger. A node of 0.10.1
//! decodes neither: an entry it cannot decode would stop its replica of
//! the group, and a snapshot it cannot decode would never install. So a
//! leader proposes the command only while every member of the group, its
//! voters and learners, runs a release that decodes it. Nothing else of
//! 0.10.2 is gated: every other entry and every snapshot of a group whose
//! ledger has not started stay byte for byte those of 0.10.1.
//!
//! **How a leader knows.** Every Raft answer a node gives carries, beside
//! its stamp (`ursula-node-id`, `ursula-incarnation`), the features its
//! binary supports in [`FEATURES_METADATA`]. A node of 0.10.1 sends none.
//! The sender records the features of each peer's latest accepted answer
//! ([`crate::PeerCluster::peer_features`]), so a peer that restarts on an
//! older binary loses them with its first answer. A leader hears every
//! member of its groups at each heartbeat. [`admit`] refuses a gated
//! command before proposal, with nothing written, while a member other
//! than the leader has not advertised the feature on its latest answer,
//! as for a member the leader has not heard since it started.
//!
//! **What it does not cover.** A gate on a proposal cannot stop a binary
//! downgrade or a member added later on an older binary: once a group's
//! ledger started, every replica of it must run 0.10.2 or later. The
//! upgrade notes say so.

use openraft::Raft;
use tonic::metadata::AsciiMetadataValue;
use tonic::metadata::MetadataMap;
use ursula_runtime::EngineOperation;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_stream::StreamCommand;

use crate::RaftCommand;
use crate::UrsulaRaftTypeConfig;

/// Answer metadata naming the features of the answering process's binary,
/// comma-separated. Absent from the answers of 0.10.1 and earlier.
pub const FEATURES_METADATA: &str = "ursula-features";

/// A feature a binary advertises on its Raft answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    /// Decodes `AckStreamUsage` and a started usage ledger's snapshot
    /// frames (0.10.2).
    StreamUsageLedger,
}

impl Feature {
    const ALL: [Self; 1] = [Self::StreamUsageLedger];

    const fn token(self) -> &'static str {
        match self {
            Self::StreamUsageLedger => "stream-usage-ledger",
        }
    }

    const fn bit(self) -> u8 {
        match self {
            Self::StreamUsageLedger => 1,
        }
    }
}

/// A set of [`Feature`]s.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Features(u8);

impl Features {
    /// What this binary supports.
    pub const fn this_binary() -> Self {
        Self(Feature::StreamUsageLedger.bit())
    }

    pub const fn contains(self, feature: Feature) -> bool {
        self.0 & feature.bit() != 0
    }

    /// The features `metadata` names. A token this binary does not know,
    /// of a later release, is skipped; a malformed value names none.
    pub(crate) fn of(metadata: &MetadataMap) -> Self {
        let Some(text) = metadata
            .get(FEATURES_METADATA)
            .and_then(|value| value.to_str().ok())
        else {
            return Self::default();
        };
        let bits = text
            .split(',')
            .filter_map(|token| {
                Feature::ALL
                    .into_iter()
                    .find(|feature| feature.token() == token.trim())
            })
            .fold(0, |bits, feature| bits | feature.bit());
        Self(bits)
    }

    /// Writes the set into `metadata`; an empty set writes nothing.
    pub(crate) fn write(self, metadata: &mut MetadataMap) {
        let tokens = Feature::ALL
            .into_iter()
            .filter(|feature| self.contains(*feature))
            .map(Feature::token)
            .collect::<Vec<_>>();
        if tokens.is_empty() {
            return;
        }
        if let Ok(value) = AsciiMetadataValue::try_from(tokens.join(",")) {
            metadata.insert(FEATURES_METADATA, value);
        }
    }
}

/// The feature every replica of a group needs before `command` may be
/// proposed to it, if any.
pub(crate) fn needs(command: &RaftCommand) -> Option<Feature> {
    match command {
        RaftCommand::Stream(command) => stream_needs(command),
        RaftCommand::Meta(_) => None,
    }
}

fn stream_needs(command: &StreamCommand) -> Option<Feature> {
    match command {
        StreamCommand::AckStreamUsage { .. } => Some(Feature::StreamUsageLedger),
        StreamCommand::IfIncarnation { command, .. } => stream_needs(command),
        _ => None,
    }
}

/// The member of `members`, other than `me`, whose features lack
/// `feature`, if any.
pub(crate) fn lacking_member(
    members: impl IntoIterator<Item = u64>,
    me: u64,
    feature: Feature,
    features_of: impl Fn(u64) -> Features,
) -> Option<u64> {
    members
        .into_iter()
        .find(|member| *member != me && !features_of(*member).contains(feature))
}

/// The gate's check before `command` is proposed on the leader `raft`:
/// refused, with nothing proposed, while a member of the group does not
/// advertise the feature it needs. `peer_cluster` is this node's; without
/// one (a test transport) only a group of this node alone passes.
pub(crate) fn admit<SM>(
    raft: &Raft<UrsulaRaftTypeConfig, SM>,
    peer_cluster: Option<&crate::PeerCluster>,
    command: &RaftCommand,
) -> Result<(), GroupEngineError>
where
    SM: openraft::storage::RaftStateMachine<UrsulaRaftTypeConfig>,
{
    let Some(feature) = needs(command) else {
        return Ok(());
    };
    let (members, me) = {
        let watch = raft.metrics();
        let metrics = openraft::rt::WatchReceiver::borrow_watched(&watch);
        let members = metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(node, _)| *node)
            .collect::<Vec<_>>();
        (members, metrics.id)
    };
    let lacking = lacking_member(members, me, feature, |member| {
        peer_cluster.map_or_else(Features::default, |peers| peers.peer_features(member))
    });
    match lacking {
        None => Ok(()),
        Some(member) => {
            tracing::debug!(
                member,
                feature = feature.token(),
                "usage gate: a member has not advertised the feature; refused before proposal"
            );
            Err(GroupEngineError::Infra(GroupInfraError::Unsupported {
                operation: EngineOperation::AckStreamUsage,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn features_round_trip_and_skip_unknown_tokens() {
        let mut metadata = MetadataMap::new();
        assert_eq!(Features::of(&metadata), Features::default());
        Features::this_binary().write(&mut metadata);
        assert_eq!(
            metadata
                .get(FEATURES_METADATA)
                .and_then(|v| v.to_str().ok()),
            Some("stream-usage-ledger")
        );
        assert!(Features::of(&metadata).contains(Feature::StreamUsageLedger));

        metadata.insert(
            FEATURES_METADATA,
            AsciiMetadataValue::from_static("a-later-feature, stream-usage-ledger"),
        );
        assert_eq!(Features::of(&metadata), Features::this_binary());
        metadata.insert(
            FEATURES_METADATA,
            AsciiMetadataValue::from_static("a-later-feature"),
        );
        assert_eq!(Features::of(&metadata), Features::default());

        // An empty set writes nothing, as a 0.10.1 answer carries nothing.
        let mut empty = MetadataMap::new();
        Features::default().write(&mut empty);
        assert!(empty.get(FEATURES_METADATA).is_none());
    }

    #[test]
    fn only_the_acknowledgement_is_gated() {
        let ack = StreamCommand::AckStreamUsage {
            history: 1,
            through_seq: 2,
        };
        assert_eq!(
            needs(&RaftCommand::Stream(ack.clone())),
            Some(Feature::StreamUsageLedger)
        );
        let wrapped = StreamCommand::IfIncarnation {
            incarnation: 3,
            command: Box::new(ack),
        };
        assert_eq!(
            needs(&RaftCommand::Stream(wrapped)),
            Some(Feature::StreamUsageLedger)
        );
        for command in ursula_runtime::wire_samples::stream_commands() {
            if !matches!(command, StreamCommand::AckStreamUsage { .. }) {
                assert_eq!(needs(&RaftCommand::Stream(command)), None);
            }
        }
    }

    #[test]
    fn a_member_without_the_feature_holds_the_gate() {
        let feature = Feature::StreamUsageLedger;
        let upgraded = |member: u64| {
            if member == 3 {
                Features::default()
            } else {
                Features::this_binary()
            }
        };
        assert_eq!(lacking_member([1, 2, 3], 1, feature, upgraded), Some(3));
        assert_eq!(lacking_member([1, 2], 1, feature, upgraded), None);
        // The leader itself runs this binary; a member it never heard holds
        // the gate.
        assert_eq!(lacking_member([3], 3, feature, upgraded), None);
        assert_eq!(
            lacking_member([1, 2], 1, feature, |_| Features::default()),
            Some(2)
        );
    }
}
