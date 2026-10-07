//! Typed administrative HTTP requests, responses and identity preconditions.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

/// A mutation must carry the incarnation observed before its maintenance plan.
pub const PROCESS_INCARNATION_HEADER: &str = "x-ursula-process-incarnation";

/// A fresh identity for one server instance, shared by its HTTP listeners.
/// This is an identity precondition, not a credential or maintenance lease.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProcessIncarnation(String);

impl ProcessIncarnation {
    pub fn from_bits(bits: u128) -> Self {
        Self(format!("{bits:032x}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProcessIncarnation {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if !canonical_identity(&value) {
            return Err("process incarnation must be 32 lowercase hexadecimal characters");
        }
        Ok(Self(value))
    }
}

fn canonical_identity(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// The body of `POST /__ursula/raft/{group}/recovery/accept-unsynced-loss`:
/// the replica's log as the operator saw it in `GET /__ursula/metrics`
/// (`last_log_index` and `current_term` of the group) when deciding to
/// accept its loss. The node refuses the request when the replica no longer
/// holds that log, so the acceptance applies to what the operator saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptUnsyncedLossRequest {
    /// The group's `last_log_index` on the replica, `null` for an empty log.
    /// Required, also when `null`.
    #[serde(deserialize_with = "required_option")]
    pub expected_last_log_index: Option<u64>,
    /// The group's `current_term` on the replica.
    pub expected_current_term: u64,
}

/// Deserializes an `Option` field that must be present, so that a missing
/// field is an error rather than `None`.
fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

impl From<ProcessIncarnation> for String {
    fn from(value: ProcessIncarnation) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::AcceptUnsyncedLossRequest;
    use super::ProcessIncarnation;

    /// The expected log must be stated, also when it is empty.
    #[test]
    fn an_accept_request_names_the_observed_log_explicitly() {
        assert_eq!(
            serde_json::from_str::<AcceptUnsyncedLossRequest>(
                r#"{"expected_last_log_index": 12, "expected_current_term": 3}"#
            )
            .unwrap(),
            AcceptUnsyncedLossRequest {
                expected_last_log_index: Some(12),
                expected_current_term: 3,
            }
        );
        assert_eq!(
            serde_json::from_str::<AcceptUnsyncedLossRequest>(
                r#"{"expected_last_log_index": null, "expected_current_term": 0}"#
            )
            .unwrap(),
            AcceptUnsyncedLossRequest {
                expected_last_log_index: None,
                expected_current_term: 0,
            }
        );
        for invalid in [
            r#"{"expected_current_term": 3}"#,
            r#"{"expected_last_log_index": 12}"#,
            r#"{"expected_last_log_index": 12, "expected_current_term": 3, "force": true}"#,
            r#"{}"#,
        ] {
            serde_json::from_str::<AcceptUnsyncedLossRequest>(invalid)
                .expect_err("an incomplete or unknown expectation must be rejected");
        }
    }

    #[test]
    fn identity_is_canonical_and_rejects_ambiguous_input() {
        let identity = ProcessIncarnation::from_bits(0xabcd);
        assert_eq!(identity.as_str(), "0000000000000000000000000000abcd");
        assert_eq!(
            ProcessIncarnation::try_from(identity.as_str().to_owned()),
            Ok(identity)
        );
        for invalid in [
            "",
            "abcd",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "\"00000000000000000000000000000000",
            "g0000000000000000000000000000000",
        ] {
            ProcessIncarnation::try_from(invalid.to_owned())
                .expect_err("a non-canonical incarnation must be rejected");
        }
    }
}

/// Machine-readable reason for a rejected leadership handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferRejection {
    NotRegistered,
    NotLeader,
    InvalidTarget,
    RecoveringTarget,
    RaftStopped,
}
impl TransferRejection {
    pub fn should_replan(self) -> bool {
        matches!(self, Self::NotLeader | Self::RecoveringTarget)
    }
}

/// Result of submitting a planned leadership transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferLeaderResponse {
    pub raft_group_id: u64,
    #[serde(default)]
    pub from: Option<u64>,
    #[serde(default)]
    pub to: Option<u64>,
    #[serde(default)]
    pub current_leader: Option<u64>,
    pub transferred: bool,
    pub rejection: Option<TransferRejection>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Request a local election against an observed term.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfElectionRequest {
    pub current_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftMaintenanceIssue {
    EmptyExpectedInventory,
    MissingGroup,
    UnexpectedGroup,
    DuplicateGroup,
    WrongNodeIdentity,
    RaftStopped,
    RecoveryBarrier,
    StoppedForOperator,
    JointMembership,
    IncompleteVoterSet,
    LocalReplicaNotVoter,
    MembershipNotApplied,
    LeaderUnknown,
    LeaderOutsideVoters,
    NotApplied,
    ApplyLag,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMaintenanceReport {
    pub version: u32,
    pub node_id: u64,
    pub lag_tolerance: u64,
    pub expected_groups: BTreeMap<u32, BTreeSet<u64>>,
    pub node_issues: Vec<RaftMaintenanceIssue>,
    pub group_issues: BTreeMap<u32, Vec<RaftMaintenanceIssue>>,
}

impl RaftMaintenanceReport {
    /// Serving ignores remote voter completeness and transitional membership.
    /// All local initialization, recovery and apply checks remain mandatory.
    pub fn serving_ready(&self) -> bool {
        let local_issue = |issue: &RaftMaintenanceIssue| {
            !matches!(
                issue,
                RaftMaintenanceIssue::IncompleteVoterSet | RaftMaintenanceIssue::JointMembership
            )
        };
        self.version == 1
            && !self.expected_groups.is_empty()
            && !self.node_issues.iter().any(local_issue)
            && !self.group_issues.values().flatten().any(local_issue)
    }

    pub fn ready(&self) -> bool {
        self.version == 1
            && !self.expected_groups.is_empty()
            && self.node_issues.is_empty()
            && self.group_issues.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftGroupMaintenanceState {
    pub running: bool,
    pub recovery_ready: bool,
    pub accepting_transfers: bool,
    pub membership_joint: bool,
    pub membership_log_index: Option<u64>,
    pub stopped_for_operator: bool,
}

/// One fresh, outbound ReadIndex confirmation bound to its committed leader.
/// Callers must wait for every required replica to apply that prefix.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QuorumPrefix {
    pub raft_group_id: u32,
    pub leader_id: u64,
    pub leader_term: u64,
    pub required_applied_index: u64,
}

/// A group's recovery gate on this replica, as status reports show it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryGateStatus {
    /// The replica votes and campaigns.
    Open,
    /// Gated; no leader has confirmed a barrier yet.
    AwaitingBarrier,
    /// Gated, and for the configured stall timeout no leader confirmed a barrier
    /// and nothing was applied: the group waits for an operator to accept
    /// the loss of the unsynced tail.
    Stalled,
    /// Gated; a leader confirmed a barrier and the replica catches up.
    CatchingUp,
}

impl RecoveryGateStatus {
    pub fn is_stalled(self) -> bool {
        self == Self::Stalled
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryGatesReport {
    #[serde(deserialize_with = "deserialize_group_map")]
    pub gated: BTreeMap<u32, RecoveryGateStatus>,
    pub stalled: Vec<u32>,
}
// Flattened serde objects buffer JSON keys as strings, losing the JSON
// deserializer's integer-key coercion. Accept both wire representations.
fn deserialize_group_map<'de, D, V>(deserializer: D) -> Result<BTreeMap<u32, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    V: Deserialize<'de>,
{
    #[derive(Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    #[serde(untagged)]
    enum Key {
        Number(u32),
        Text(String),
    }
    let entries = BTreeMap::<Key, V>::deserialize(deserializer)?;
    let mut groups = BTreeMap::new();
    for (key, value) in entries {
        let group = match key {
            Key::Number(group) => group,
            Key::Text(text) => text.parse().map_err(serde::de::Error::custom)?,
        };
        if groups.insert(group, value).is_some() {
            return Err(serde::de::Error::custom("duplicate group identifier"));
        }
    }
    Ok(groups)
}

impl RecoveryGatesReport {
    pub fn new(gates: BTreeMap<u32, RecoveryGateStatus>) -> Self {
        let gated: BTreeMap<_, _> = gates
            .into_iter()
            .filter(|(_, status)| *status != RecoveryGateStatus::Open)
            .collect();
        let stalled = gated
            .iter()
            .filter(|(_, status)| status.is_stalled())
            .map(|(group, _)| *group)
            .collect();
        Self { gated, stalled }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeadershipShedStatus {
    pub bits: u8,
    pub state: String,
    pub should_accept_transfer: bool,
    pub should_campaign: bool,
    pub recovery_barriers_ready: bool,
    pub should_shed_current_leaders: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    pub format_version: u32,
    pub raft_group_count: u32,
}
/// Answer of `POST /__ursula/backup/group/{group}/cold-check`, whose body is
/// one backup group object: whether the answering node's cold store holds
/// every cold object the group references. `ursulactl restore` asks it for
/// every group before the first import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupColdCheck {
    pub raft_group_id: u32,
    /// Distinct cold objects the group references, index pages included.
    pub referenced_objects: u64,
    /// How many of them the cold store does not hold.
    pub missing_objects: u64,
    /// Some missing keys, relative to `storage.cold.root`.
    pub missing_sample: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub raft_group_id: u32,
    pub snapshot_index: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurgeResponse {
    pub raft_group_id: u32,
    pub purged_index: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddLearnerResponse {
    pub raft_group_id: u32,
    pub node_id: u64,
    pub log_index: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipResponse {
    pub raft_group_id: u32,
    #[serde(default)]
    pub voter_ids: BTreeSet<u64>,
    pub log_index: Option<u64>,
    pub current_leader: Option<u64>,
    pub changed: bool,
    pub reason: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurgeQuery {
    pub upto: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddLearnerQuery {
    pub addr: String,
    pub blocking: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipQuery {
    pub voters: String,
}

/// Shared node metrics contract. Missing safety fields must never default to
/// healthy; runtime and transport counters use the shared telemetry schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub process_incarnation: Option<ProcessIncarnation>,
    pub process_node_id: Option<u64>,
    pub raft_groups: Vec<RaftGroupMetrics>,
    pub raft_maintenance: Option<RaftMaintenanceReport>,
    #[serde(flatten)]
    pub diagnostics: crate::telemetry::NodeDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftGroupMetrics {
    #[serde(default)]
    pub apply_failure: Option<RaftApplyFailure>,
    pub raft_group_id: u64,
    pub node_id: u64,
    pub current_term: Option<u64>,
    pub current_leader: Option<u64>,
    pub committed_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub voter_ids: Vec<u64>,
    pub learner_ids: Vec<u64>,
    pub maintenance: RaftGroupMaintenanceState,
    pub last_log_index: Option<u64>,
    pub committed_term: Option<u64>,
    pub last_applied_term: Option<u64>,
    pub snapshot_term: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub purged_term: Option<u64>,
    pub purged_index: Option<u64>,
    #[serde(default)]
    pub log_bytes_since_snapshot: u64,
    #[serde(default)]
    pub log_entries_since_snapshot: u64,
    #[serde(default)]
    pub last_snapshot_bytes: u64,
    #[serde(default)]
    pub has_snapshot: bool,
}

pub const MAINTENANCE_READINESS_PATH: &str = "/__ursula/maintenance/ready";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceReadiness {
    pub ready: bool,
    pub raft_maintenance: Option<RaftMaintenanceReport>,
}

#[cfg(test)]
mod metrics_contract_tests {
    use super::*;

    #[test]
    fn missing_safety_fields_are_rejected() {
        let node = serde_json::json!({});
        serde_json::from_value::<NodeMetrics>(node).expect_err("missing group inventory is unsafe");
        let group = serde_json::json!({"raft_group_id": 0, "node_id": 1, "voter_ids": [1], "learner_ids": []});
        serde_json::from_value::<RaftGroupMetrics>(group)
            .expect_err("missing maintenance is unsafe");
    }

    #[test]
    fn flattened_metrics_round_trip_nonempty_recovery_gates() {
        let metrics = NodeMetrics {
            process_incarnation: Some(ProcessIncarnation::from_bits(1)),
            process_node_id: Some(1),
            raft_groups: vec![],
            raft_maintenance: None,
            diagnostics: crate::telemetry::NodeDiagnostics {
                recovery_gates: Some(RecoveryGatesReport::new(BTreeMap::from([
                    (0, RecoveryGateStatus::AwaitingBarrier),
                    (u32::MAX, RecoveryGateStatus::Stalled),
                ]))),
                ..Default::default()
            },
        };
        let wire = serde_json::to_vec(&metrics).unwrap();
        let decoded: NodeMetrics = serde_json::from_slice(&wire).unwrap();
        assert_eq!(
            decoded.diagnostics.recovery_gates.unwrap().gated,
            metrics.diagnostics.recovery_gates.unwrap().gated
        );
    }

    #[test]
    fn serving_survives_incomplete_voters_but_maintenance_does_not() {
        let mut report = RaftMaintenanceReport {
            version: 1,
            node_id: 1,
            lag_tolerance: 0,
            expected_groups: BTreeMap::from([(0, BTreeSet::from([1, 2, 3]))]),
            node_issues: vec![],
            group_issues: BTreeMap::from([(0, vec![
                RaftMaintenanceIssue::IncompleteVoterSet,
                RaftMaintenanceIssue::JointMembership,
            ])]),
        };
        assert!(report.serving_ready());
        assert!(!report.ready());
        for issue in [
            RaftMaintenanceIssue::RecoveryBarrier,
            RaftMaintenanceIssue::ApplyLag,
            RaftMaintenanceIssue::NotApplied,
            RaftMaintenanceIssue::MissingGroup,
        ] {
            report.group_issues.insert(0, vec![issue]);
            assert!(!report.serving_ready());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftApplyFailure {
    pub term: u64,
    pub index: u64,
    pub kind: RaftApplyFailureKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RaftApplyFailureKind {
    Panic,
    Infrastructure,
}
