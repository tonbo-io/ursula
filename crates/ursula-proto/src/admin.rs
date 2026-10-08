//! Typed administrative HTTP requests, responses and identity preconditions.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

/// A mutation must carry the incarnation observed before its maintenance plan.
pub const PROCESS_INCARNATION_HEADER: &str = "x-ursula-process-incarnation";

/// The immutable executor token admitted by the cell's maintenance reservation.
pub const MAINTENANCE_FENCE_HEADER: &str = "x-ursula-maintenance-fence";

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

/// One admitted executor of a maintenance operation. Generations increase
/// across both takeovers and subsequent operations for the entire cell.
/// This token supplies ordering, not authentication or a reservation store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MaintenanceFenceFields")]
pub struct MaintenanceFence {
    reservation_id: String,
    executor_id: String,
    generation: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceFenceFields {
    reservation_id: String,
    executor_id: String,
    generation: u64,
}

impl MaintenanceFence {
    pub fn new(
        reservation_id: String,
        executor_id: String,
        generation: u64,
    ) -> Result<Self, &'static str> {
        if !canonical_identity(&reservation_id) || !canonical_identity(&executor_id) {
            return Err("maintenance identities must be 32 lowercase hexadecimal characters");
        }
        if generation == 0 {
            return Err("maintenance generation must be nonzero");
        }
        Ok(Self {
            reservation_id,
            executor_id,
            generation,
        })
    }

    pub fn reservation_id(&self) -> &str {
        &self.reservation_id
    }

    pub fn executor_id(&self) -> &str {
        &self.executor_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn header_value(&self) -> String {
        format!(
            "{}:{}:{}",
            self.reservation_id, self.generation, self.executor_id
        )
    }
}

impl TryFrom<MaintenanceFenceFields> for MaintenanceFence {
    type Error = &'static str;

    fn try_from(value: MaintenanceFenceFields) -> Result<Self, Self::Error> {
        Self::new(value.reservation_id, value.executor_id, value.generation)
    }
}

impl FromStr for MaintenanceFence {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split(':');
        let reservation = parts.next().ok_or("missing reservation identity")?;
        let generation_text = parts.next().ok_or("missing maintenance generation")?;
        let executor = parts.next().ok_or("missing executor identity")?;
        let generation = generation_text
            .parse::<u64>()
            .map_err(|_invalid| "invalid maintenance generation")?;
        if parts.next().is_some() || generation.to_string() != generation_text {
            return Err("maintenance fence header is not canonical");
        }
        Self::new(reservation.to_owned(), executor.to_owned(), generation)
    }
}

/// Process-local executor admission. Retirement retains the generation so a
/// delayed activation cannot reopen the released executor's authority.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MaintenanceFenceState {
    /// No reservation protocol has been installed; this is uncertified.
    #[default]
    Unclaimed,
    /// The persistent ownership protocol is installed, with no completed
    /// reservation yet. Mutations remain closed until an executor activates.
    AwaitingReservation,
    Active {
        fence: MaintenanceFence,
    },
    Activating {
        fence: MaintenanceFence,
    },
    Retiring {
        fence: MaintenanceFence,
    },
    Retired {
        fence: MaintenanceFence,
    },
}

impl MaintenanceFenceState {
    pub fn fence(&self) -> Option<&MaintenanceFence> {
        match self {
            Self::Unclaimed | Self::AwaitingReservation => None,
            Self::Active { fence }
            | Self::Activating { fence }
            | Self::Retiring { fence }
            | Self::Retired { fence } => Some(fence),
        }
    }
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

/// A trusted startup helper's acknowledged ownership, loaded before listeners.
/// The server generates the incarnation; the helper must return it unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupAdmission {
    pub process_incarnation: ProcessIncarnation,
    pub maintenance_fence: MaintenanceFenceState,
}

impl StartupAdmission {
    /// Startup never grants active mutation authority or uncertified admission.
    pub fn validate(&self) -> Result<(), &'static str> {
        match self.maintenance_fence {
            MaintenanceFenceState::AwaitingReservation
            | MaintenanceFenceState::Activating { .. }
            | MaintenanceFenceState::Retired { .. } => Ok(()),
            _ => Err("startup admission must retain closed maintenance authority"),
        }
    }

    pub fn start_maintenance_drained(&self) -> bool {
        matches!(
            self.maintenance_fence,
            MaintenanceFenceState::Activating { .. }
        )
    }
}

impl From<ProcessIncarnation> for String {
    fn from(value: ProcessIncarnation) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::AcceptUnsyncedLossRequest;
    use super::MaintenanceFence;
    use super::ProcessIncarnation;
    use super::RaftMaintenanceIssue;
    use super::RaftMaintenanceReport;
    use super::TransferRejection;

    /// A newer server of the same minor version may report an issue or a
    /// rejection this build does not know. It decodes, never reads as ready
    /// and is never retried.
    #[test]
    fn unknown_issues_and_rejections_decode_and_fail_closed() {
        let report: RaftMaintenanceReport = serde_json::from_str(
            r#"{"version": 1, "node_id": 1, "lag_tolerance": 0,
                "expected_groups": {"0": [1, 2, 3]},
                "node_issues": ["a_future_issue"], "group_issues": {}}"#,
        )
        .unwrap();
        assert_eq!(report.node_issues, vec![RaftMaintenanceIssue::Unknown]);
        assert!(!report.ready());
        let rejection: TransferRejection = serde_json::from_str(r#""a_future_rejection""#).unwrap();
        assert_eq!(rejection, TransferRejection::Unknown);
        assert!(!rejection.should_replan());
        assert_eq!(
            serde_json::from_str::<TransferRejection>(r#""not_leader""#).unwrap(),
            TransferRejection::NotLeader
        );
    }

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
    fn fence_header_rejects_ambiguous_generations_and_identities() {
        let id = "00000000000000000000000000000001";
        let fence = MaintenanceFence::new(id.to_owned(), id.to_owned(), u64::MAX).unwrap();
        assert_eq!(fence.header_value().parse::<MaintenanceFence>(), Ok(fence));
        for generation in ["0", "01", "+1", "-1", " 1", "18446744073709551616", ""] {
            format!("{id}:{generation}:{id}")
                .parse::<MaintenanceFence>()
                .expect_err("a non-canonical or out-of-range generation must be rejected");
        }
        for value in [
            format!("{id}:1"),
            format!("{id}:1:{id}:extra"),
            format!("bad:1:{id}"),
            format!("{id}:1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        ] {
            value
                .parse::<MaintenanceFence>()
                .expect_err("a malformed fence header must be rejected");
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
    /// The target has not answered the leader recently or lacks committed
    /// entries, so the handoff could not complete now.
    UnreadyTarget,
    RaftStopped,
    /// A rejection a newer server of the same minor version reports that this
    /// build does not know. It is not retried.
    #[serde(other)]
    Unknown,
}
impl TransferRejection {
    pub fn should_replan(self) -> bool {
        matches!(
            self,
            Self::NotLeader | Self::RecoveringTarget | Self::UnreadyTarget
        )
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
    MembershipNotApplied,
    LeaderUnknown,
    LeaderOutsideVoters,
    NotApplied,
    ApplyLag,
    /// An issue a newer server of the same minor version reports that this
    /// build does not know. Like every issue, it makes the report not ready.
    #[serde(other)]
    Unknown,
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
    pub process_incarnation: ProcessIncarnation,
    #[serde(deserialize_with = "Option::deserialize")]
    pub process_node_id: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub maintenance_fence: Option<MaintenanceFenceState>,
    pub maintenance_fence_uncertain: bool,
    pub raft_groups: Vec<RaftGroupMetrics>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub raft_maintenance: Option<RaftMaintenanceReport>,
    #[serde(flatten)]
    pub diagnostics: crate::telemetry::NodeDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftGroupMetrics {
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

#[cfg(test)]
mod metrics_contract_tests {
    use super::*;

    fn node() -> NodeMetrics {
        NodeMetrics {
            process_incarnation: ProcessIncarnation::from_bits(1),
            process_node_id: Some(1),
            maintenance_fence: Some(MaintenanceFenceState::Unclaimed),
            maintenance_fence_uncertain: false,
            raft_groups: vec![],
            raft_maintenance: Some(RaftMaintenanceReport {
                version: 1,
                node_id: 1,
                lag_tolerance: 0,
                expected_groups: BTreeMap::from([(0, BTreeSet::from([1]))]),
                node_issues: vec![],
                group_issues: BTreeMap::new(),
            }),
            diagnostics: crate::telemetry::NodeDiagnostics::default(),
        }
    }

    #[test]
    fn absent_safety_fields_are_not_healthy_defaults() {
        for field in [
            "process_incarnation",
            "process_node_id",
            "raft_groups",
            "raft_maintenance",
            "maintenance_fence",
            "maintenance_fence_uncertain",
        ] {
            let mut wire = serde_json::to_value(node()).unwrap();
            wire.as_object_mut().unwrap().remove(field);
            serde_json::from_value::<NodeMetrics>(wire).expect_err(field);
        }
        serde_json::from_value::<RaftGroupMetrics>(serde_json::json!({
            "raft_group_id": 0, "node_id": 1, "voter_ids": [1], "learner_ids": []
        }))
        .expect_err("group participation proof is required");
    }

    #[test]
    fn unsupported_maintenance_version_is_not_a_safety_proof() {
        let mut node = node();
        node.raft_maintenance.as_mut().unwrap().version = 2;
        let decoded: NodeMetrics =
            serde_json::from_value(serde_json::to_value(node).unwrap()).unwrap();
        assert!(!decoded.raft_maintenance.unwrap().ready());
    }

    #[test]
    fn flattened_metrics_round_trip_nonempty_recovery_gates_and_diagnostics() {
        let mut node = node();
        node.diagnostics.recovery_gates = Some(RecoveryGatesReport::new(BTreeMap::from([
            (0, RecoveryGateStatus::AwaitingBarrier),
            (u32::MAX, RecoveryGateStatus::Stalled),
        ])));
        node.diagnostics.runtime.accepted_appends = 7;
        node.diagnostics.raft_grpc.raft_grpc_append_stream_requests = 19;
        let wire = serde_json::to_vec(&node).unwrap();
        let decoded: NodeMetrics = serde_json::from_slice(&wire).unwrap();
        assert_eq!(decoded.diagnostics.runtime.accepted_appends, 7);
        assert_eq!(
            decoded
                .diagnostics
                .raft_grpc
                .raft_grpc_append_stream_requests,
            19
        );
        assert_eq!(
            decoded.diagnostics.recovery_gates.unwrap().gated,
            node.diagnostics.recovery_gates.unwrap().gated
        );
    }
}
