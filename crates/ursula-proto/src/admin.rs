//! Typed administrative HTTP requests, responses and identity preconditions.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

/// A wire schema version validated during decoding; unsupported versions cannot
/// enter the operation's typed state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct SchemaVersion<const VERSION: u32>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedSchemaVersion {
    pub expected: u32,
    pub observed: u32,
}
impl std::fmt::Display for UnsupportedSchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unsupported schema version {}; expected {}",
            self.observed, self.expected
        )
    }
}
impl std::error::Error for UnsupportedSchemaVersion {}
impl<const V: u32> TryFrom<u32> for SchemaVersion<V> {
    type Error = UnsupportedSchemaVersion;
    fn try_from(observed: u32) -> Result<Self, Self::Error> {
        if observed == V {
            Ok(Self)
        } else {
            Err(UnsupportedSchemaVersion {
                expected: V,
                observed,
            })
        }
    }
}
impl<const V: u32> From<SchemaVersion<V>> for u32 {
    fn from(_: SchemaVersion<V>) -> Self {
        V
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ApplyFailure {
    pub term: u64,
    pub index: u64,
    pub kind: ApplyFailureKind,
    pub message: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyFailureKind {
    Panic,
    InvariantViolation,
    /// A kind this build does not know, which an admin-plane reader of
    /// another minor's metrics may meet. The group is still stopped. It does
    /// not round-trip: it encodes as `unknown`, not as the kind it stands
    /// for. Admin JSON only: `GroupEngineError::ApplyStopped` carries the
    /// closed `ursula_runtime::ApplyStopKind` over the Raft RPC, since
    /// `#[serde(other)]` is never allowed in the MessagePack closure.
    #[serde(other)]
    Unknown,
}

/// A mutation must carry the process incarnation
/// ([`crate::placement::ProcessIncarnation`]) it observed before its plan.
pub const PROCESS_INCARNATION_HEADER: &str = "x-ursula-process-incarnation";

/// A replica's log as the recovery tool read it in `GET /__ursula/metrics`:
/// what a recovery plan expects each member to still hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaLogView {
    /// The group's `last_log.index` on the replica, `null` for an empty log.
    /// Required, also when `null`.
    #[serde(deserialize_with = "required_option")]
    pub last_log_index: Option<u64>,
    /// The group's `last_log.term` on the replica, `null` for an empty log.
    /// Required, also when `null`.
    #[serde(deserialize_with = "required_option")]
    pub last_log_term: Option<u64>,
    /// The group's `current_term` on the replica.
    pub current_term: u64,
}

/// The body of `POST /__ursula/raft/{target}/recovery/accept-unsynced-loss`,
/// where `target` is `meta` or a data group's id: a recovery plan that
/// accepts the loss of the group's unsynced tail (#581 section 4.5). The tool reads every reachable member and refuses
/// unless a majority of the voters answer. It sends the plan to the leader
/// first ([`PlanStep::Lead`]), which answers its plan term, and then to every
/// other reachable member ([`PlanStep::Follow`] with that term). A member
/// that takes it votes only for `leader`, and only at a term above the plan
/// term, until it applied a history event at a log index above `point`. The
/// leader proposes that event first and serves nothing before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPlanRequest {
    /// The member the plan elects: the greatest last log id, or with a cut
    /// the greatest committed index.
    pub leader: u64,
    /// The leader's last log index after the plan (its committed index
    /// with a cut).
    pub point: u64,
    /// Whether every member keeps only the entries it knew were committed:
    /// the leader's last log term is below the highest vote term a gated
    /// member holds, so its log may hold a stale entry.
    pub cut: bool,
    /// Whether this replica leads the plan or follows it.
    pub step: PlanStep,
    /// What the plan keeps at least, by the plane it is for. A replica
    /// refuses a floor of the other plane
    /// ([`RecoveryPlanRefusal::FloorOtherPlane`]).
    pub floor: PlanFloor,
    /// With a cut whose `point` is above the plan leader's own commit
    /// marker: the id of the entry at `point`, which another replica reported
    /// committed (#613). The leader keeps its log up to `point` only if it
    /// holds exactly that entry; every other member cuts to its own commit
    /// marker as always.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_entry: Option<EntryId>,
    /// This replica's log as the tool read it.
    pub expected: ReplicaLogView,
}

/// What a recovery plan keeps at least, by the plane it is for. Required in
/// every plan: the tool decides it before it sends anything.
///
/// - `meta`: a plan of the meta group. Meta is planned first, since the
///   floor of a data group is read from a confirmed meta view.
/// - `seeded`: a plan of a data group that a confirmed meta view shows
///   seeded, so no membership change of the group was ever certified and
///   nothing above the plan's own cut needs keeping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanFloor {
    Meta,
    Seeded,
}

/// The id of one log entry: its leader's term and node, and its index is
/// given with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryId {
    pub term: u64,
    pub node_id: u64,
}

/// Which part of a recovery plan a request carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum PlanStep {
    /// To the plan's leader, first: it takes the plan and answers its plan
    /// term ([`RecoveryPlanReport::leader_term`]). `highest_term` is the
    /// highest `current_term` the tool read on any replica it reached. The
    /// plan term is the higher of it and the leader's own vote term: a
    /// leader at or below it gets nothing applied after the plan, and one
    /// above it cannot make the plan's leader win, since every replica that
    /// may vote for it either holds the plan or is open and unplanned and
    /// refuses a log behind its own.
    Lead { highest_term: u64 },
    /// To every other member, after the leader took the plan: it votes for
    /// the leader only at a term above `leader_term`, which only campaigns
    /// the leader starts after it took the plan reach.
    Follow { leader_term: u64 },
}

/// The body of a `409 Conflict` from `accept-unsynced-loss`: why the replica
/// refused the plan. It changed nothing, except after `cut_refused`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryPlanRefused {
    pub raft_group_id: u64,
    pub node_id: u64,
    pub refusal: RecoveryPlanRefusal,
    /// The refusal in words, for a person.
    pub message: String,
}

/// Why a replica refused a recovery plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum RecoveryPlanRefusal {
    /// The replica lost its vote and holds no vote floor yet.
    MissingVoteFloor,
    /// The replica leads the group with a live quorum.
    LeadsLive,
    /// The replica follows a live leader, `leader`.
    FollowsLeader { leader: u64 },
    /// The request's `step` is not this replica's.
    StepMismatch,
    /// The plan's leader is not a voter of the membership it would campaign
    /// under: its effective membership, or with a cut the one it keeps.
    /// Only the plan's leader answers it, and it changed nothing. The tool
    /// reads the replicas again and plans again.
    LeaderNotVoter,
    /// The replica's gate may still open without a plan, or runs a cut.
    NotStalled { status: RecoveryGateStatus },
    /// The replica no longer holds the log the plan's `expected` names.
    ReplicaChanged,
    /// The replica committed or applied an entry above the plan's point.
    RecoveryPointBehind,
    /// The plan's leader does not continue from the plan's point.
    PointMismatch,
    /// The plan cuts and the replica knows no committed entry.
    NothingCommitted,
    /// The plan cuts and the replica no longer holds the entry it keeps.
    CommittedEntryMissing,
    /// The plan's cut could not run at the group's restart: the plan was
    /// dropped, the log recorded recovering and the gate stays closed. The
    /// one refusal that changed the replica: it took the plan first.
    CutRefused,
    /// The plan's cut left its leader, this replica, no voter of the
    /// membership it keeps: it could never campaign. Like `cut_refused`, the
    /// plan was dropped, the log recorded recovering and the gate stays
    /// closed. Read the replicas again and plan again.
    CutLeftNoVote,
    /// The plan's cut restarted its leader, this replica, and the restarted
    /// core never showed the membership it keeps in time. Like
    /// `cut_left_no_vote`, the plan was dropped. Read the replicas again and
    /// plan again.
    CutMembershipUnknown,
    /// The replica's log store closed.
    StoreClosed,
    /// The replica is taking another recovery plan.
    PlanInFlight,
    /// The plan cuts a replica whose voter sets name only itself
    /// ([`single_voter`]): no other voter could have overwritten an entry it
    /// holds, so its log is kept whole.
    SingleVoterCut,
    /// A `follow` names a plan term below the vote term the tool read on
    /// this replica: the term did not come from the plan's leader.
    PlanTermBelowVote,
    /// The plan's `floor` is of the other plane: `meta` sent to a data
    /// group, or `seeded` sent to the meta group. It changed nothing.
    FloorOtherPlane,
}

/// Whether a replica's voter sets, of its effective membership and of the
/// one it keeps after a cut, name a single voter (#581 section 4.5, #613).
/// Every entry above its commit marker was then written while it was the
/// only voter, so no other voter could have overwritten one, and a recovery
/// plan never cuts its log. The planner and the server both decide with it.
pub fn single_voter<'a>(sets: impl IntoIterator<Item = &'a BTreeSet<u64>>) -> bool {
    let mut voters = sets.into_iter().flatten();
    match voters.next() {
        Some(first) => voters.all(|voter| voter == first),
        None => false,
    }
}

impl RecoveryPlanRefusal {
    /// Whether no plan can fix the refusal: a replica that lost its vote
    /// needs a quorum's proof first, and a step that is not the replica's
    /// is a bug in the tool, as is a floor of the other plane.
    /// `leader_not_voter` is not one: a plan read from the current
    /// membership names another leader (#613).
    pub fn no_plan_fixes(&self) -> bool {
        matches!(
            self,
            Self::MissingVoteFloor | Self::StepMismatch | Self::FloorOtherPlane
        )
    }
}

/// What a recovery plan did on one replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPlanOutcome {
    /// The replica is the plan's leader: it campaigns and proposes the
    /// history event once elected.
    Leads,
    /// The replica votes only for the plan's leader until the event applies.
    Follows,
}

/// The answer of `POST /__ursula/raft/{target}/recovery/accept-unsynced-loss`
/// on one node. The meta group answers as group 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryPlanReport {
    pub raft_group_id: u32,
    pub node_id: u64,
    pub outcome: RecoveryPlanOutcome,
    /// The plan this replica holds.
    pub leader: u64,
    pub point: u64,
    /// The plan term: the leader's vote term when it took the plan.
    pub leader_term: u64,
}

/// Deserializes an `Option` field that must be present, so that a missing
/// field is an error rather than `None`. Every admin JSON type's `Option`
/// fields decode through it.
pub fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::ApplyFailure;
    use super::ApplyFailureKind;
    use super::RaftMaintenanceIssue;
    use super::RaftMaintenanceReport;
    use super::TransferRejection;

    /// An admin-plane reader of another minor's metrics may meet a failure
    /// kind it does not know. The stopped group's diagnostics still decode.
    #[test]
    fn unknown_apply_failure_kinds_decode() {
        let failure: ApplyFailure = serde_json::from_str(
            r#"{"term": 2, "index": 9, "kind": "a_future_kind", "message": "stopped"}"#,
        )
        .unwrap();
        assert_eq!(failure.kind, ApplyFailureKind::Unknown);
        assert_eq!(
            serde_json::from_str::<ApplyFailureKind>(r#""invariant_violation""#).unwrap(),
            ApplyFailureKind::InvariantViolation
        );
    }

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

    /// Golden JSON (#581 sections 4.4 and 4.5): the recovery plan, its
    /// report and the import's answer are the admin API between `ursulactl`
    /// and the server, so their wire form never changes within a minor.
    #[test]
    fn recovery_plans_and_import_answers_keep_their_wire_form() {
        use super::BackupImportResponse;
        use super::PlanStep;
        use super::RecoveryPlanOutcome;
        use super::RecoveryPlanReport;
        use super::RecoveryPlanRequest;
        use super::ReplicaLogView;
        let plan = RecoveryPlanRequest {
            point_entry: None,
            leader: 2,
            point: 12,
            cut: true,
            step: PlanStep::Follow { leader_term: 9 },
            floor: super::PlanFloor::Seeded,
            expected: ReplicaLogView {
                last_log_index: Some(14),
                last_log_term: Some(3),
                current_term: 5,
            },
        };
        assert_eq!(
            serde_json::to_string(&plan).unwrap(),
            r#"{"leader":2,"point":12,"cut":true,"step":{"follow":{"leader_term":9}},"floor":"seeded","expected":{"last_log_index":14,"last_log_term":3,"current_term":5}}"#
        );
        let lead = RecoveryPlanRequest {
            point_entry: None,
            step: PlanStep::Lead { highest_term: 8 },
            floor: super::PlanFloor::Meta,
            ..plan
        };
        assert_eq!(
            serde_json::to_string(&lead).unwrap(),
            r#"{"leader":2,"point":12,"cut":true,"step":{"lead":{"highest_term":8}},"floor":"meta","expected":{"last_log_index":14,"last_log_term":3,"current_term":5}}"#
        );
        for request in [plan, lead] {
            assert_eq!(
                serde_json::from_str::<RecoveryPlanRequest>(
                    &serde_json::to_string(&request).unwrap()
                )
                .unwrap(),
                request
            );
        }
        let refused = super::RecoveryPlanRefused {
            raft_group_id: 4,
            node_id: 3,
            refusal: super::RecoveryPlanRefusal::FollowsLeader { leader: 2 },
            message: "follows".to_owned(),
        };
        assert_eq!(
            serde_json::to_string(&refused).unwrap(),
            r#"{"raft_group_id":4,"node_id":3,"refusal":{"reason":"follows_leader","leader":2},"message":"follows"}"#
        );
        assert_eq!(
            serde_json::to_string(&super::RecoveryPlanRefusal::ReplicaChanged).unwrap(),
            r#"{"reason":"replica_changed"}"#
        );
        assert_eq!(
            serde_json::to_string(&super::RecoveryPlanRefusal::FloorOtherPlane).unwrap(),
            r#"{"reason":"floor_other_plane"}"#
        );
        let report = RecoveryPlanReport {
            raft_group_id: 4,
            node_id: 2,
            outcome: RecoveryPlanOutcome::Follows,
            leader: 2,
            point: 12,
            leader_term: 9,
        };
        assert_eq!(
            serde_json::to_string(&report).unwrap(),
            r#"{"raft_group_id":4,"node_id":2,"outcome":"follows","leader":2,"point":12,"leader_term":9}"#
        );
        let answer = BackupImportResponse {
            buckets: 1,
            streams: 3,
            group_commit_index: 9,
            history: std::num::NonZeroU64::new(0x0123_4567_89ab_cdef).unwrap(),
            already: false,
        };
        assert_eq!(
            serde_json::to_string(&answer).unwrap(),
            r#"{"buckets":1,"streams":3,"group_commit_index":9,"history":81985529216486895,"already":false}"#
        );
        assert_eq!(
            serde_json::from_str::<BackupImportResponse>(&serde_json::to_string(&answer).unwrap())
                .unwrap(),
            answer
        );
    }

    /// A recovery plan states every member's expected log, also when it is
    /// empty, and nothing else.
    #[test]
    fn a_recovery_plan_names_the_observed_log_explicitly() {
        use super::RecoveryPlanRequest;
        use super::ReplicaLogView;
        assert_eq!(
            serde_json::from_str::<RecoveryPlanRequest>(
                r#"{"leader": 2, "point": 12, "cut": false, "step": {"lead": {"highest_term": 4}},
                    "floor": "seeded",
                    "expected": {"last_log_index": 12, "last_log_term": 3, "current_term": 4}}"#
            )
            .unwrap(),
            RecoveryPlanRequest {
                point_entry: None,
                leader: 2,
                point: 12,
                cut: false,
                step: super::PlanStep::Lead { highest_term: 4 },
                floor: super::PlanFloor::Seeded,
                expected: ReplicaLogView {
                    last_log_index: Some(12),
                    last_log_term: Some(3),
                    current_term: 4,
                },
            }
        );
        assert_eq!(
            serde_json::from_str::<ReplicaLogView>(
                r#"{"last_log_index": null, "last_log_term": null, "current_term": 0}"#
            )
            .unwrap(),
            ReplicaLogView {
                last_log_index: None,
                last_log_term: None,
                current_term: 0,
            }
        );
        for invalid in [
            r#"{"expected_last_log_index": 12, "expected_current_term": 3}"#,
            r#"{"leader": 2, "point": 12, "expected":
                {"last_log_index": 12, "last_log_term": 3, "current_term": 4}}"#,
            r#"{"leader": 2, "point": 12, "cut": false, "expected":
                {"last_log_index": 12, "current_term": 4}}"#,
            r#"{"leader": 2, "point": 12, "cut": false, "floor": "seeded", "force": true,
                "step": {"lead": {"highest_term": 4}}, "expected":
                {"last_log_index": 12, "last_log_term": 3, "current_term": 4}}"#,
            // A 0.9 plan names no floor.
            r#"{"leader": 2, "point": 12, "cut": false, "step": {"lead": {"highest_term": 4}},
                "expected": {"last_log_index": 12, "last_log_term": 3, "current_term": 4}}"#,
            r#"{"leader": 2, "point": 12, "cut": false, "step": {"lead": {"highest_term": 4}},
                "floor": "certified",
                "expected": {"last_log_index": 12, "last_log_term": 3, "current_term": 4}}"#,
        ] {
            serde_json::from_str::<RecoveryPlanRequest>(invalid).expect_err(
                "a 0.7 or 0.9 body, an incomplete plan, an unknown floor or an unknown field is \
                 refused",
            );
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
        matches!(self, Self::NotLeader | Self::UnreadyTarget)
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
    ApplyStopped,
    RecoveryBarrier,
    StoppedForOperator,
    JointMembership,
    IncompleteVoterSet,
    /// The group's membership names a node that is not one of its
    /// configured voters. No node dials a member that `raft.peers` does not
    /// list, and the leader never removes it: an operator lists the node
    /// again, or replaces the voter set without it through the membership
    /// endpoint, which also removes every learner outside the new voters.
    UnconfiguredMember,
    LocalReplicaNotVoter,
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
    pub version: SchemaVersion<1>,
    pub node_id: u64,
    pub lag_tolerance: u64,
    pub expected_groups: BTreeMap<u32, BTreeSet<u64>>,
    pub node_issues: Vec<RaftMaintenanceIssue>,
    pub group_issues: BTreeMap<u32, Vec<RaftMaintenanceIssue>>,
}

impl RaftMaintenanceReport {
    /// Local serving eligibility; maintenance additionally requires the full
    /// voter set, no member outside it, and no stopped group. A group
    /// stopped by an apply failure answers its own requests with a typed
    /// 503 while the node serves the others, so it only blocks disruption.
    pub fn serving_ready(&self) -> bool {
        let local_issue = |issue: &RaftMaintenanceIssue| {
            !matches!(
                issue,
                RaftMaintenanceIssue::IncompleteVoterSet
                    | RaftMaintenanceIssue::UnconfiguredMember
                    | RaftMaintenanceIssue::JointMembership
                    | RaftMaintenanceIssue::ApplyStopped
            )
        };
        !self.expected_groups.is_empty()
            && !self.node_issues.iter().any(local_issue)
            && !self.group_issues.values().flatten().any(local_issue)
    }

    pub fn ready(&self) -> bool {
        !self.expected_groups.is_empty()
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
    #[serde(deserialize_with = "Option::deserialize")]
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
    /// A recovery plan holds the replica: it votes only for the plan's
    /// leader until the plan's history event applies (#581 section 4.5).
    Planned,
    /// A recovery plan elects this replica: it campaigns and votes for no
    /// other candidate until the plan's history event applies.
    LeadsPlan,
    /// The replica took a plan that cuts its log and its group restarts to
    /// run the cut: it votes for no one and takes no replication.
    Cutting,
}

impl RecoveryGateStatus {
    pub fn is_stalled(self) -> bool {
        self == Self::Stalled
    }
}

/// A Raft group of either plane as admin JSON names it: the meta group as
/// `"meta"`, a data group by its id, a number. As a JSON object key the id
/// is a string, like every key. The kernel's form is
/// `ursula_shard::RaftTarget`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RaftTargetKey {
    Data(u32),
    Meta,
}

/// The admin JSON name of the meta group.
const META_KEY: &str = "meta";

/// `meta`, or the group id: the key's JSON form, as an admin route names it.
impl std::fmt::Display for RaftTargetKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Data(group) => write!(f, "{group}"),
            Self::Meta => f.write_str(META_KEY),
        }
    }
}

impl Serialize for RaftTargetKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Data(group) => serializer.serialize_u32(*group),
            Self::Meta => serializer.serialize_str(META_KEY),
        }
    }
}

impl<'de> Deserialize<'de> for RaftTargetKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = RaftTargetKey;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("\"meta\" or a raft group id")
            }

            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
                u32::try_from(value)
                    .map(RaftTargetKey::Data)
                    .map_err(|_overflow| {
                        E::invalid_value(serde::de::Unexpected::Unsigned(value), &self)
                    })
            }

            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
                u32::try_from(value)
                    .map(RaftTargetKey::Data)
                    .map_err(|_overflow| {
                        E::invalid_value(serde::de::Unexpected::Signed(value), &self)
                    })
            }

            /// `"meta"`, or a group id as an object key carries it.
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value == META_KEY {
                    return Ok(RaftTargetKey::Meta);
                }
                value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit())
                    .then(|| value.parse().ok())
                    .flatten()
                    .map(RaftTargetKey::Data)
                    .ok_or_else(|| E::invalid_value(serde::de::Unexpected::Str(value), &self))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// The recovery gates of this node's replicas that are not open, the meta
/// replica's under `"meta"`, and the groups among them that stalled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryGatesReport {
    pub gated: BTreeMap<RaftTargetKey, RecoveryGateStatus>,
    pub stalled: Vec<RaftTargetKey>,
}
impl RecoveryGatesReport {
    pub fn new(gates: BTreeMap<RaftTargetKey, RecoveryGateStatus>) -> Self {
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

/// Answer of `GET /__ursula/leadership-shed`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeadershipShedStatus {
    /// The answering node's `server.cluster_id`. A poller trusts only an
    /// answer of its own cluster.
    pub cluster_id: ursula_headers::ClusterId,
    pub bits: u8,
    pub state: String,
    pub should_accept_transfer: bool,
    pub should_campaign: bool,
    pub recovery_barriers_ready: bool,
    pub should_shed_current_leaders: bool,
}
/// Answer of `POST /__ursula/backup/group/{group}/import`. The import is a
/// history event keyed by the backup's BLAKE3 digest (#581 section 4.4): a
/// repeat of an import the group already holds answers `already: true` and
/// changes nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupImportResponse {
    pub buckets: u64,
    pub streams: u64,
    pub group_commit_index: u64,
    /// The id of the history the import started: every restored stream has
    /// a new token, and every restored producer record is history-fenced.
    pub history: std::num::NonZeroU64,
    /// The group already held this import.
    pub already: bool,
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
pub struct MembershipQuery {
    pub voters: String,
}

/// Shared operational metrics. Diagnostic extensions are intentionally ignored
/// by readers, so new server diagnostics cannot break maintenance tooling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// The node's `server.cluster_id` (`Ursula-Cluster-Id`): every node of
    /// one deployment reports the same.
    pub cluster_id: ursula_headers::ClusterId,
    pub process_incarnation: crate::placement::ProcessIncarnation,
    #[serde(deserialize_with = "Option::deserialize")]
    pub process_node_id: Option<u64>,
    #[serde(rename = "raft_groups")]
    pub groups: Vec<RaftGroupMetrics>,
    /// This node's meta replica, `null` when it runs none. Apart from
    /// `raft_groups`, where its group 0 would meet data group 0.
    #[serde(deserialize_with = "Option::deserialize")]
    pub meta: Option<RaftGroupMetrics>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub raft_maintenance: Option<RaftMaintenanceReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftGroupMetrics {
    pub raft_group_id: u64,
    pub node_id: u64,
    pub current_term: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    pub current_leader: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub committed_index: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub last_applied_index: Option<u64>,
    pub voter_ids: Vec<u64>,
    /// The voter configs of the effective membership: one, or two while a
    /// change is joint. A recovery plan needs a majority of each (#613).
    pub voter_configs: Vec<BTreeSet<u64>>,
    /// The voter configs of the membership the replica keeps when a
    /// recovery plan cuts its log back to its committed entry: the
    /// effective one once it is committed, else the one before it.
    pub committed_voter_configs: Vec<BTreeSet<u64>>,
    pub learner_ids: Vec<u64>,
    pub maintenance: RaftGroupMaintenanceState,
    #[serde(deserialize_with = "Option::deserialize")]
    pub last_log_index: Option<u64>,
    /// The term of the last log entry, `null` only for an empty log. With
    /// `last_log_index` it is the last log id an election compares.
    #[serde(deserialize_with = "Option::deserialize")]
    pub last_log_term: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub committed_term: Option<u64>,
    /// The node that led the term of the committed entry: with
    /// `committed_term` and `committed_index`, the entry's id.
    #[serde(deserialize_with = "Option::deserialize")]
    pub committed_node: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub last_applied_term: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub snapshot_term: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub snapshot_index: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub purged_term: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub purged_index: Option<u64>,
    pub log_bytes_since_snapshot: u64,
    pub log_entries_since_snapshot: u64,
    pub last_snapshot_bytes: u64,
    pub has_snapshot: bool,
    /// Set once a committed command failed to apply: the replica stopped.
    #[serde(deserialize_with = "Option::deserialize")]
    pub apply_failure: Option<ApplyFailure>,
    /// The history event this replica applied last (#581 section 4.3),
    /// `null` before the group's genesis.
    #[serde(deserialize_with = "Option::deserialize")]
    pub history: Option<RaftGroupHistory>,
    /// The recovery plan this replica holds until its history event
    /// applies (#581 section 4.5).
    #[serde(deserialize_with = "Option::deserialize")]
    pub recovery: Option<RaftGroupRecovery>,
    /// The replica's recovery gate for this group: `open` when it has none.
    pub recovery_gate: RecoveryGateStatus,
    /// The replica leads the group, and a quorum acknowledged it within the
    /// last 3 seconds. A leader that lost its quorum stays a leader in its
    /// own metrics; this says it is not live (m2, #581 review).
    pub leads_live: bool,
}

/// The history event a replica applied last.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftGroupHistory {
    /// The log index of the entry that applied it. A recovery plan is
    /// satisfied on a replica once this is above the plan's point.
    pub event_index: u64,
    /// The group was restored from a backup.
    pub imported: bool,
}

/// A recovery plan a replica holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftGroupRecovery {
    pub leader: u64,
    pub point: u64,
    /// The plan term members require on the leader's vote requests.
    pub leader_term: u64,
    /// The plan's cut has not run yet.
    pub cut_pending: bool,
}
impl RaftGroupMetrics {
    pub fn participation_ready(&self) -> bool {
        let health = &self.maintenance;
        self.apply_failure.is_none()
            && health.running
            && health.recovery_ready
            && !health.membership_joint
            && !health.stopped_for_operator
            && health.membership_log_index.is_some()
            && self.last_applied_index >= health.membership_log_index
            && self.committed_index.is_some()
    }
}

#[cfg(test)]
mod metrics_contract_tests {
    use super::*;

    fn complete() -> NodeMetrics {
        NodeMetrics {
            cluster_id: ursula_headers::ClusterId::new("prod-eu-1").unwrap(),
            process_incarnation: crate::placement::ProcessIncarnation {
                root: crate::placement::RootId::new(std::num::NonZeroU64::MIN),
                run: 1,
                nonce: 2,
            },
            process_node_id: Some(1),
            meta: None,
            raft_maintenance: Some(RaftMaintenanceReport {
                version: SchemaVersion,
                node_id: 1,
                lag_tolerance: 0,
                expected_groups: BTreeMap::from([(0, BTreeSet::from([1]))]),
                node_issues: vec![],
                group_issues: BTreeMap::new(),
            }),
            groups: vec![RaftGroupMetrics {
                raft_group_id: 0,
                node_id: 1,
                current_term: 1,
                current_leader: Some(1),
                committed_index: Some(5),
                last_applied_index: Some(5),
                voter_ids: vec![1],
                voter_configs: vec![BTreeSet::from([1])],
                committed_voter_configs: vec![BTreeSet::from([1])],
                learner_ids: vec![],
                maintenance: RaftGroupMaintenanceState {
                    running: true,
                    recovery_ready: true,
                    accepting_transfers: true,
                    membership_joint: false,
                    membership_log_index: Some(0),
                    stopped_for_operator: false,
                },
                last_log_index: Some(5),
                last_log_term: Some(1),
                committed_term: Some(1),
                committed_node: Some(1),
                last_applied_term: Some(1),
                snapshot_term: None,
                snapshot_index: None,
                purged_term: None,
                purged_index: None,
                log_bytes_since_snapshot: 0,
                log_entries_since_snapshot: 0,
                last_snapshot_bytes: 0,
                has_snapshot: false,
                apply_failure: None,
                history: None,
                recovery: None,
                recovery_gate: RecoveryGateStatus::Open,
                leads_live: false,
            }],
        }
    }

    #[test]
    fn every_emitted_operational_field_is_required() {
        let wire = serde_json::to_value(complete()).unwrap();
        for path in [
            "",
            "/raft_groups/0",
            "/raft_groups/0/maintenance",
            "/raft_maintenance",
        ] {
            for key in wire.pointer(path).unwrap().as_object().unwrap().keys() {
                let mut damaged = wire.clone();
                damaged
                    .pointer_mut(path)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .remove(key);
                let error = serde_json::from_value::<NodeMetrics>(damaged).unwrap_err();
                assert!(error.is_data(), "missing {path}/{key}: {error}");
            }
        }
    }

    #[test]
    fn null_is_only_accepted_for_explicitly_nullable_fields() {
        let wire = serde_json::to_value(complete()).unwrap();
        for path in [
            "/process_incarnation",
            "/raft_groups",
            "/raft_groups/0/current_term",
            "/raft_groups/0/maintenance",
            "/raft_groups/0/voter_ids",
            "/raft_groups/0/voter_configs",
            "/raft_groups/0/committed_voter_configs",
            "/raft_groups/0/has_snapshot",
            "/raft_groups/0/log_bytes_since_snapshot",
            "/raft_groups/0/log_entries_since_snapshot",
            "/raft_groups/0/last_snapshot_bytes",
        ] {
            let mut damaged = wire.clone();
            *damaged.pointer_mut(path).unwrap() = serde_json::Value::Null;
            assert!(
                serde_json::from_value::<NodeMetrics>(damaged)
                    .unwrap_err()
                    .is_data(),
                "{path}"
            );
        }
        let mut nullable = complete();
        nullable.process_node_id = None;
        nullable.raft_maintenance = None;
        nullable.groups[0].committed_index = None;
        let decoded: NodeMetrics =
            serde_json::from_value(serde_json::to_value(nullable).unwrap()).unwrap();
        assert!(!decoded.groups[0].participation_ready());
    }

    #[test]
    fn diagnostic_extensions_do_not_participate_in_operational_decoding() {
        let mut wire = serde_json::to_value(complete()).unwrap();
        wire["wal_recovery"] =
            serde_json::json!({"fsync":"future-policy", "previous_run":"future-variant"});
        wire["runtime_future_counter"] = serde_json::json!({"arbitrary": [1,2]});
        let decoded: NodeMetrics = serde_json::from_value(wire).unwrap();
        assert!(decoded.groups[0].participation_ready());
        assert!(decoded.raft_maintenance.unwrap().ready());
    }

    #[test]
    fn schema_versions_are_validated_at_the_wire_boundary() {
        assert_eq!(
            SchemaVersion::<1>::try_from(2),
            Err(UnsupportedSchemaVersion {
                expected: 1,
                observed: 2
            })
        );
        let mut wire = serde_json::to_value(complete()).unwrap();
        wire["raft_maintenance"]["version"] = serde_json::json!(2);
        assert!(
            serde_json::from_value::<NodeMetrics>(wire)
                .unwrap_err()
                .is_data()
        );
    }
}

/// Local serving health, distinct from complete maintenance redundancy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServingReadinessReason {
    FormatEpochMismatch,
    WalDiskPressure,
    RecoveryStalled,
    RecoveryGateClosed,
    RaftReplicaUnready,
    /// A reason a newer server of the same minor version reports that this
    /// build does not know. The response status still decides readiness.
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServingReadiness {
    pub ready: bool,
    #[serde(deserialize_with = "Option::deserialize")]
    pub reason: Option<ServingReadinessReason>,
    /// The node's `server.cluster_id`.
    pub cluster_id: ursula_headers::ClusterId,
    /// Configured peers whose Raft traffic was refused for another cluster
    /// id within the last minute, either way. Never a `reason` and never a
    /// cause of unreadiness: a misconfigured node must not take its healthy
    /// peers out of service, nor hide why a node is unready.
    pub peer_cluster_mismatches: Vec<PeerClusterMismatch>,
    pub format_epoch_mismatch: bool,
    pub recovery_barriers_ready: bool,
    #[serde(deserialize_with = "Option::deserialize")]
    pub raft_maintenance: Option<RaftMaintenanceReport>,
    pub recovery_stalled_groups: Vec<u32>,
    pub wal_disk_pressure: bool,
    pub wal_available_bytes: u64,
    pub wal_min_available_bytes: u64,
    pub wal_resume_available_bytes: u64,
    pub wal_disk_stat_errors: u64,
}

/// A configured peer seen with another cluster id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerClusterMismatch {
    pub node_id: u64,
    /// The cluster id the peer sent or answered with.
    pub cluster_id: ursula_headers::ClusterId,
}

#[cfg(test)]
mod serving_readiness_tests {
    use super::*;

    #[test]
    fn readiness_reasons_keep_their_wire_names_and_unknown_reasons_decode() {
        for (reason, name) in [
            (
                ServingReadinessReason::FormatEpochMismatch,
                "format_epoch_mismatch",
            ),
            (ServingReadinessReason::WalDiskPressure, "wal_disk_pressure"),
            (ServingReadinessReason::RecoveryStalled, "recovery_stalled"),
            (
                ServingReadinessReason::RecoveryGateClosed,
                "recovery_gate_closed",
            ),
            (
                ServingReadinessReason::RaftReplicaUnready,
                "raft_replica_unready",
            ),
        ] {
            assert_eq!(serde_json::to_value(&reason).unwrap(), name);
            assert_eq!(
                serde_json::from_value::<ServingReadinessReason>(name.into()).unwrap(),
                reason
            );
        }
        let newer: ServingReadinessReason =
            serde_json::from_str("\"reason_from_a_newer_server\"").unwrap();
        assert_eq!(newer, ServingReadinessReason::Unknown);
    }

    #[test]
    fn only_membership_completeness_is_relaxed_for_serving() {
        let mut report = RaftMaintenanceReport {
            version: SchemaVersion,
            node_id: 1,
            lag_tolerance: 16,
            expected_groups: BTreeMap::from([(0, BTreeSet::from([1, 2, 3]))]),
            node_issues: vec![],
            group_issues: BTreeMap::new(),
        };
        for issue in [
            RaftMaintenanceIssue::IncompleteVoterSet,
            RaftMaintenanceIssue::UnconfiguredMember,
            RaftMaintenanceIssue::JointMembership,
        ] {
            report.group_issues.insert(0, vec![issue]);
            assert!(report.serving_ready());
            assert!(!report.ready());
        }
        for issue in [
            RaftMaintenanceIssue::LocalReplicaNotVoter,
            RaftMaintenanceIssue::RecoveryBarrier,
            RaftMaintenanceIssue::RaftStopped,
            RaftMaintenanceIssue::ApplyLag,
            RaftMaintenanceIssue::MembershipNotApplied,
            RaftMaintenanceIssue::MissingGroup,
            RaftMaintenanceIssue::LeaderUnknown,
            RaftMaintenanceIssue::LeaderOutsideVoters,
        ] {
            report.group_issues.insert(0, vec![issue]);
            assert!(!report.serving_ready());
        }
        report.group_issues.clear();
        report.expected_groups.clear();
        assert!(!report.serving_ready());
    }
}

/// Answer of `GET /__ursula/usage`: per-bucket usage on the answering node,
/// and the staged snapshot parts of the groups it leads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageReport {
    pub version: SchemaVersion<1>,
    /// The answering node's id, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<u64>,
    /// The unit of `committed_write_units`, in bytes.
    pub write_unit_bytes: u64,
    /// From replicated state, summed across the node's Raft groups: every
    /// replica reports the same values.
    pub buckets: BTreeMap<String, BucketUsageReport>,
    /// How many Raft groups the cluster has, numbered from 0.
    pub raft_group_count: u32,
    /// The staged, unused snapshot parts of each group the node leads, in
    /// group order. Node-local, not replicated: a collector keeps, per
    /// group, the report of the highest term, and treats as partial a group
    /// whose counting has not finished or that no report names.
    #[serde(default)]
    pub staged_snapshot_parts: Vec<GroupStagedParts>,
}

/// One bucket's usage in a [`UsageReport`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketUsageReport {
    pub committed_append_bytes: u64,
    pub committed_records: u64,
    pub committed_write_units: u64,
    pub retained_bytes: u64,
    pub stream_count: u64,
}

/// The staged snapshot parts of one group, as its leader counts them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupStagedParts {
    pub raft_group_id: u32,
    /// The Raft term the answering node leads the group in.
    pub term: u64,
    /// The leader finished its listing of the group's parts. Until then
    /// `bytes` holds only what it counted so far.
    pub counting_finished: bool,
    /// Bytes of staged, unused parts per bucket.
    pub bytes: BTreeMap<String, u64>,
}

impl UsageReport {
    /// The staged-part bytes per bucket across `reports`, one per node, as
    /// a collector sums them: for each group the report of the highest term
    /// counts, and `false` beside the sum when one of those has not finished
    /// counting, or when no report names one of the cluster's groups.
    pub fn staged_part_bytes(reports: &[Self]) -> (BTreeMap<String, u64>, bool) {
        let mut newest = BTreeMap::<u32, &GroupStagedParts>::new();
        for group in reports
            .iter()
            .flat_map(|report| &report.staged_snapshot_parts)
        {
            match newest.get(&group.raft_group_id) {
                Some(seen) if seen.term >= group.term => {}
                _ => {
                    newest.insert(group.raft_group_id, group);
                }
            }
        }
        let mut buckets = BTreeMap::<String, u64>::new();
        let group_count = reports
            .iter()
            .map(|report| report.raft_group_count)
            .max()
            .unwrap_or(0);
        let mut complete = (0..group_count).all(|group| newest.contains_key(&group));
        for group in newest.values() {
            complete &= group.counting_finished;
            for (bucket_id, bytes) in &group.bytes {
                let total = buckets.entry(bucket_id.clone()).or_default();
                *total = total.saturating_add(*bytes);
            }
        }
        (buckets, complete)
    }
}

#[cfg(test)]
mod usage_tests {
    use std::collections::BTreeMap;

    use super::GroupStagedParts;
    use super::SchemaVersion;
    use super::UsageReport;

    fn report(node_id: u64, groups: Vec<GroupStagedParts>) -> UsageReport {
        UsageReport {
            version: SchemaVersion,
            node_id: Some(node_id),
            write_unit_bytes: 10_240,
            buckets: BTreeMap::new(),
            raft_group_count: 2,
            staged_snapshot_parts: groups,
        }
    }

    fn group(raft_group_id: u32, term: u64, finished: bool, bytes: u64) -> GroupStagedParts {
        GroupStagedParts {
            raft_group_id,
            term,
            counting_finished: finished,
            bytes: BTreeMap::from([("tenant".to_owned(), bytes)]),
        }
    }

    /// A deposed leader that has not seen the new term still reports its
    /// group. The collector keeps the higher term per group, so nothing is
    /// counted twice, and a group whose counting has not finished makes the
    /// sum partial.
    #[test]
    fn a_collector_keeps_the_newest_term_per_group() {
        let reports = [
            report(1, vec![group(0, 7, true, 100), group(1, 3, true, 5)]),
            report(2, vec![group(0, 8, true, 40)]),
        ];
        assert_eq!(
            UsageReport::staged_part_bytes(&reports),
            (BTreeMap::from([("tenant".to_owned(), 45)]), true)
        );
        let reports = [
            report(1, vec![group(0, 7, true, 100), group(1, 3, true, 5)]),
            report(2, vec![group(0, 8, false, 40)]),
        ];
        assert_eq!(
            UsageReport::staged_part_bytes(&reports),
            (BTreeMap::from([("tenant".to_owned(), 45)]), false)
        );
        // Group 1 has no leader any report names: the sum is partial.
        let reports = [report(1, vec![group(0, 7, true, 100)])];
        assert_eq!(
            UsageReport::staged_part_bytes(&reports),
            (BTreeMap::from([("tenant".to_owned(), 100)]), false)
        );
    }
}

#[cfg(test)]
mod recovery_gates_report_tests {
    use std::collections::BTreeMap;

    use super::RaftTargetKey;
    use super::RecoveryGateStatus;
    use super::RecoveryGatesReport;

    /// The report names the meta group `"meta"` and a data group by its id,
    /// a string as an object key and a number in the stalled list, and
    /// decodes back to itself. Meta's gate is never read as data group 0's.
    #[test]
    fn the_report_names_each_plane_and_round_trips() {
        let report = RecoveryGatesReport::new(BTreeMap::from([
            (RaftTargetKey::Data(0), RecoveryGateStatus::Open),
            (RaftTargetKey::Data(7), RecoveryGateStatus::AwaitingBarrier),
            (RaftTargetKey::Data(9), RecoveryGateStatus::Stalled),
            (RaftTargetKey::Meta, RecoveryGateStatus::Stalled),
        ]));
        let json = serde_json::to_string(&report).expect("the report renders");
        assert_eq!(
            json,
            r#"{"gated":{"7":"awaiting_barrier","9":"stalled","meta":"stalled"},"stalled":[9,"meta"]}"#
        );
        let decoded: RecoveryGatesReport = serde_json::from_str(&json).expect("the report decodes");
        assert_eq!(decoded, report);
    }

    #[test]
    fn a_key_that_names_no_group_does_not_decode() {
        for key in [
            r#""Meta""#,
            r#""+7""#,
            r#""-1""#,
            "4294967296",
            "-1",
            r#""""#,
        ] {
            assert!(
                serde_json::from_str::<RaftTargetKey>(key).is_err(),
                "{key} decoded"
            );
        }
        assert_eq!(
            serde_json::from_str::<RaftTargetKey>("4294967295").expect("a group id"),
            RaftTargetKey::Data(u32::MAX)
        );
    }
}
