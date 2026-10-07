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
    use super::MaintenanceFence;
    use super::ProcessIncarnation;

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
