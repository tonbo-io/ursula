//! The typed target of a Raft RPC (format epoch 6): the plane and group a
//! request names, and the node it is for.
//!
//! Every Raft request carries the plane next to its frozen `raft_group_id`
//! (`RaftPlaneV1`, where `UNSPECIFIED` is refused) and the node it is for
//! (`node_id` on the envelope, snapshot and leader-transfer requests,
//! `target_node` on `GroupWrite`, `GroupRead` and `RejoinBarrier`).
//! [`RaftTarget`] itself is `ursula_shard`'s, which errors and recovery
//! reports share; this module adds its wire form as [`WireTarget`]:
//! [`WireTarget::stamp`] is the one encoder of those fields, for every
//! request builder and the format-epoch probe's envelope, and
//! [`WireTarget::parse`] their one parser. A receiver checks the protocol
//! version first, then the target, then looks the group up.
//!
//! A request for another node is refused with `FAILED_PRECONDITION` and the
//! refusing node's id in its [`NODE_ID_METADATA`] status metadata: the
//! address now serves another node. An `AppendStream` item, which has no
//! status metadata, carries the id in its refusal's `refusing_node`, and
//! the sender puts it back into the status metadata, so the refusal reads
//! alike on every RPC. The code alone already means a protocol
//! mismatch (by message text) or another cluster (by `ursula-cluster-id`
//! metadata), so a sender reads this refusal only through
//! [`refusing_node`], and never records it as a format-epoch mismatch. The
//! mismatch text decides first: a status that carries it is a version
//! refusal, never a misdelivery, whatever metadata it carries.

use tonic::metadata::AsciiMetadataValue;
use tonic::metadata::MetadataMap;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;

use crate::grpc::GrpcRpcError;
use crate::peer_cluster::NODE_ID_METADATA;
use crate::raft_internal_proto as pb;
use crate::types::RaftCommand;

/// The request metadata of an `AppendStream` naming the plane of every item
/// it carries: `data` or `meta`. A receiver refuses an item of another
/// plane, so no item bypasses its plane's receive budget.
pub const RAFT_PLANE_METADATA: &str = "ursula-raft-plane";

/// The target fields of a Raft request, as the wire carries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TargetFields {
    pub(crate) plane: i32,
    pub(crate) raft_group_id: u32,
    pub(crate) target_node: u64,
    /// The core and shard of a request that names a placement.
    pub(crate) core_and_shard: Option<(u32, u32)>,
}

/// A Raft request that names a target: its plane, group and target node.
pub(crate) trait TargetedRequest {
    /// The request's name in refusals.
    const NAME: &'static str;

    fn target_fields(&self) -> TargetFields;

    fn set_target_fields(&mut self, plane: pb::RaftPlaneV1, raft_group_id: u32, target_node: u64);
}

/// The wire form of a [`RaftTarget`]: the plane and group a Raft request
/// names, and the plane predicate of a command.
pub(crate) trait WireTarget: Sized {
    /// The plane the target is on.
    fn plane(self) -> pb::RaftPlaneV1;

    /// The `raft_group_id` the wire carries: the data group's id, 0 for the
    /// meta group.
    fn wire_group_id(self) -> u32;

    /// Whether a request of this target may carry `command`: a data group
    /// takes only stream commands, the meta group only meta commands. The
    /// only plane predicate of a command: a `GroupWrite` of the other plane
    /// is refused before anything is proposed, and an inbound append before
    /// OpenRaft sees it.
    fn admits(self, command: &RaftCommand) -> bool;

    /// Whether a recovery plan of this target may carry `floor`: a data
    /// group's plan takes `seeded`, the meta group's `meta`. The only plane
    /// predicate of a plan floor: a plan of the other plane is refused
    /// before its job runs ([`crate::RecoveryGateError::FloorOtherPlane`]).
    fn takes_floor(self, floor: ursula_proto::admin::PlanFloor) -> bool;

    /// Writes this target and `target_node` into `request`: the one encoder
    /// of the plane, group and target node of every Raft request.
    fn stamp<R: TargetedRequest>(self, target_node: u64, request: &mut R);

    /// The target `request` names, for a receiver whose node id is `local`.
    ///
    /// Refuses (`INVALID_ARGUMENT`) a plane that is `UNSPECIFIED` or
    /// unknown, and `META` with a group, core or shard other than 0; refuses
    /// a request for another node than `local` as misdelivered
    /// ([`GrpcRpcError::misdelivered`]). Any data group id parses: the group
    /// lookup after it answers `NOT_FOUND` for a group this node does not
    /// run, which the format-epoch probe's group `u32::MAX` relies on.
    fn parse<R: TargetedRequest>(request: &R, local: u64) -> Result<Self, GrpcRpcError>;

    /// The value of [`RAFT_PLANE_METADATA`] for this target's plane.
    fn plane_metadata(self) -> AsciiMetadataValue;
}

impl WireTarget for RaftTarget {
    fn plane(self) -> pb::RaftPlaneV1 {
        match self {
            Self::Data(_) => pb::RaftPlaneV1::Data,
            Self::Meta => pb::RaftPlaneV1::Meta,
        }
    }

    fn wire_group_id(self) -> u32 {
        match self {
            Self::Data(group) => group.0,
            Self::Meta => 0,
        }
    }

    fn admits(self, command: &RaftCommand) -> bool {
        match (self, command) {
            (Self::Data(_), RaftCommand::Stream(_)) | (Self::Meta, RaftCommand::Meta(_)) => true,
            (Self::Data(_), RaftCommand::Meta(_)) | (Self::Meta, RaftCommand::Stream(_)) => false,
        }
    }

    fn takes_floor(self, floor: ursula_proto::admin::PlanFloor) -> bool {
        use ursula_proto::admin::PlanFloor;
        match (self, floor) {
            (Self::Data(_), PlanFloor::Seeded) | (Self::Meta, PlanFloor::Meta) => true,
            (Self::Data(_), PlanFloor::Meta) | (Self::Meta, PlanFloor::Seeded) => false,
        }
    }

    fn stamp<R: TargetedRequest>(self, target_node: u64, request: &mut R) {
        request.set_target_fields(self.plane(), self.wire_group_id(), target_node);
    }

    fn parse<R: TargetedRequest>(request: &R, local: u64) -> Result<Self, GrpcRpcError> {
        let fields = request.target_fields();
        let target = match pb::RaftPlaneV1::try_from(fields.plane) {
            Ok(pb::RaftPlaneV1::Data) => Self::Data(RaftGroupId(fields.raft_group_id)),
            Ok(pb::RaftPlaneV1::Meta) => {
                if fields.raft_group_id != 0 {
                    return Err(GrpcRpcError::invalid_argument(format!(
                        "{} names the meta plane with raft group {}: the meta group is group 0",
                        R::NAME,
                        fields.raft_group_id
                    )));
                }
                if let Some((core, shard)) = fields.core_and_shard
                    && (core, shard) != (0, 0)
                {
                    return Err(GrpcRpcError::invalid_argument(format!(
                        "{} names the meta plane with core {core} and shard {shard}: the meta \
                         group has neither",
                        R::NAME
                    )));
                }
                Self::Meta
            }
            Ok(pb::RaftPlaneV1::Unspecified) => {
                return Err(GrpcRpcError::invalid_argument(format!(
                    "{} names no Raft plane",
                    R::NAME
                )));
            }
            Err(_unknown) => {
                return Err(GrpcRpcError::invalid_argument(format!(
                    "{} names an unknown Raft plane {}",
                    R::NAME,
                    fields.plane
                )));
            }
        };
        check_target_node(fields.target_node, local)?;
        Ok(target)
    }

    fn plane_metadata(self) -> AsciiMetadataValue {
        AsciiMetadataValue::from_static(match self {
            Self::Data(_) => "data",
            Self::Meta => "meta",
        })
    }
}

/// The plane an `AppendStream`'s [`RAFT_PLANE_METADATA`] names, or `None`
/// when it names none or an unknown one. A stream without a plane has each
/// item refused once its protocol version passed.
pub(crate) fn stream_plane(metadata: &MetadataMap) -> Option<pb::RaftPlaneV1> {
    match metadata.get(RAFT_PLANE_METADATA)?.to_str().ok()? {
        "data" => Some(pb::RaftPlaneV1::Data),
        "meta" => Some(pb::RaftPlaneV1::Meta),
        _ => None,
    }
}

/// Refuses a request for `target_node` on the node `local` as misdelivered.
pub(crate) fn check_target_node(target_node: u64, local: u64) -> Result<(), GrpcRpcError> {
    if target_node == local {
        return Ok(());
    }
    Err(GrpcRpcError::misdelivered(local, target_node))
}

/// The node that refused an outbound request as meant for another node,
/// read from the refusal's [`NODE_ID_METADATA`]: the address dialed now
/// serves that node. `None` for every other status, a protocol-version
/// refusal ([`crate::format_epoch::is_protocol_mismatch`]) among them even
/// when it carries that metadata. The only reader of the misdelivery
/// refusal.
pub(crate) fn refusing_node(status: &tonic::Status) -> Option<u64> {
    if status.code() != tonic::Code::FailedPrecondition
        || crate::format_epoch::is_protocol_mismatch(status)
    {
        return None;
    }
    status
        .metadata()
        .get(NODE_ID_METADATA)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

macro_rules! targeted_by_node_id {
    ($($request:ident),+ $(,)?) => {$(
        impl TargetedRequest for pb::$request {
            const NAME: &'static str = stringify!($request);

            fn target_fields(&self) -> TargetFields {
                TargetFields {
                    plane: self.plane,
                    raft_group_id: self.raft_group_id,
                    target_node: self.node_id,
                    core_and_shard: None,
                }
            }

            fn set_target_fields(
                &mut self,
                plane: pb::RaftPlaneV1,
                raft_group_id: u32,
                target_node: u64,
            ) {
                self.set_plane(plane);
                self.raft_group_id = raft_group_id;
                self.node_id = target_node;
            }
        }
    )+};
}

targeted_by_node_id!(
    RaftRpcEnvelopeV1,
    RaftFullSnapshotRequestV1,
    RaftTransferLeaderRequestV1,
);

macro_rules! targeted_by_target_node {
    ($($request:ident => $placement:expr),+ $(,)?) => {$(
        impl TargetedRequest for pb::$request {
            const NAME: &'static str = stringify!($request);

            fn target_fields(&self) -> TargetFields {
                let placement: fn(&Self) -> Option<(u32, u32)> = $placement;
                TargetFields {
                    plane: self.plane,
                    raft_group_id: self.raft_group_id,
                    target_node: self.target_node,
                    core_and_shard: placement(self),
                }
            }

            fn set_target_fields(
                &mut self,
                plane: pb::RaftPlaneV1,
                raft_group_id: u32,
                target_node: u64,
            ) {
                self.set_plane(plane);
                self.raft_group_id = raft_group_id;
                self.target_node = target_node;
            }
        }
    )+};
}

targeted_by_target_node!(
    GroupWriteRequestV1 => |request| Some((request.core_id, request.shard_id)),
    GroupReadRequestV1 => |request| Some((request.core_id, request.shard_id)),
    RejoinBarrierRequestV1 => |_| None,
);

#[cfg(test)]
mod tests {
    use ursula_shard::RaftGroupId;

    use super::RaftTarget;
    use super::TargetedRequest;
    use super::WireTarget;
    use super::refusing_node;
    use crate::raft_internal_proto as pb;

    /// One request of each kind, with every other field set, stamped for
    /// `target` and node `node`.
    fn stamped<R: TargetedRequest>(mut request: R, target: RaftTarget, node: u64) -> R {
        target.stamp(node, &mut request);
        request
    }

    /// A parse of one stamped request, answering the refusal's code.
    type Parse = Box<dyn Fn() -> Result<RaftTarget, tonic::Code>>;

    fn requests_for(target: RaftTarget, node: u64) -> Vec<Parse> {
        fn parsed<R: TargetedRequest + 'static>(request: R, local: u64) -> Parse {
            Box::new(move || {
                RaftTarget::parse(&request, local)
                    .map_err(|error| tonic::Status::from(error).code())
            })
        }
        vec![
            parsed(
                stamped(
                    pb::RaftRpcEnvelopeV1 {
                        protocol_version: 6,
                        payload: vec![1].into(),
                        ..Default::default()
                    },
                    target,
                    node,
                ),
                node,
            ),
            parsed(
                stamped(
                    pb::RaftFullSnapshotRequestV1 {
                        protocol_version: 6,
                        vote: vec![2].into(),
                        ..Default::default()
                    },
                    target,
                    node,
                ),
                node,
            ),
            parsed(
                stamped(
                    pb::RaftTransferLeaderRequestV1 {
                        protocol_version: 6,
                        request: vec![3].into(),
                        ..Default::default()
                    },
                    target,
                    node,
                ),
                node,
            ),
            parsed(
                stamped(
                    pb::GroupWriteRequestV1 {
                        protocol_version: 6,
                        command_payloads: vec![vec![4].into()],
                        ..Default::default()
                    },
                    target,
                    node,
                ),
                node,
            ),
            parsed(
                stamped(
                    pb::GroupReadRequestV1 {
                        protocol_version: 6,
                        bucket_id: "bucket".to_owned(),
                        ..Default::default()
                    },
                    target,
                    node,
                ),
                node,
            ),
            parsed(
                stamped(
                    pb::RejoinBarrierRequestV1 {
                        protocol_version: 6,
                        ..Default::default()
                    },
                    target,
                    node,
                ),
                node,
            ),
        ]
    }

    /// Every request kind parses back to the target it was stamped with,
    /// a data group of any id (the format-epoch probe's `u32::MAX` among
    /// them) and the meta group.
    #[test]
    fn every_request_parses_back_to_its_stamped_target() {
        for target in [
            RaftTarget::Data(RaftGroupId(0)),
            RaftTarget::Data(RaftGroupId(7)),
            RaftTarget::Data(RaftGroupId(u32::MAX)),
            RaftTarget::Meta,
        ] {
            for parse in requests_for(target, 3) {
                assert_eq!(parse(), Ok(target));
            }
        }
    }

    #[test]
    fn stamp_writes_the_plane_group_and_target_node() {
        let envelope = stamped(
            pb::RaftRpcEnvelopeV1::default(),
            RaftTarget::Data(RaftGroupId(9)),
            4,
        );
        assert_eq!(
            (envelope.plane(), envelope.raft_group_id, envelope.node_id),
            (pb::RaftPlaneV1::Data, 9, 4)
        );
        let write = stamped(pb::GroupWriteRequestV1::default(), RaftTarget::Meta, 5);
        assert_eq!(
            (write.plane(), write.raft_group_id, write.target_node),
            (pb::RaftPlaneV1::Meta, 0, 5)
        );
    }

    /// A request that names no plane never reaches data group 0.
    #[test]
    fn a_request_without_a_plane_is_refused() {
        let unspecified = pb::RaftRpcEnvelopeV1 {
            node_id: 2,
            ..Default::default()
        };
        assert_eq!(
            RaftTarget::parse(&unspecified, 2).map_err(|error| tonic::Status::from(error).code()),
            Err(tonic::Code::InvalidArgument)
        );
        let unknown = pb::RejoinBarrierRequestV1 {
            plane: 9,
            target_node: 2,
            ..Default::default()
        };
        assert_eq!(
            RaftTarget::parse(&unknown, 2).map_err(|error| tonic::Status::from(error).code()),
            Err(tonic::Code::InvalidArgument)
        );
    }

    /// The meta group is group 0, with no core or shard.
    #[test]
    fn a_meta_request_naming_another_group_is_refused() {
        let mut envelope = pb::RaftRpcEnvelopeV1 {
            raft_group_id: 1,
            node_id: 2,
            ..Default::default()
        };
        envelope.set_plane(pb::RaftPlaneV1::Meta);
        assert_eq!(
            RaftTarget::parse(&envelope, 2).map_err(|error| tonic::Status::from(error).code()),
            Err(tonic::Code::InvalidArgument)
        );
        let mut write = pb::GroupWriteRequestV1 {
            core_id: 1,
            target_node: 2,
            ..Default::default()
        };
        write.set_plane(pb::RaftPlaneV1::Meta);
        assert_eq!(
            RaftTarget::parse(&write, 2).map_err(|error| tonic::Status::from(error).code()),
            Err(tonic::Code::InvalidArgument)
        );
    }

    /// A request for another node is refused with `FAILED_PRECONDITION`,
    /// the refusing node's id in its status metadata, which
    /// `refusing_node` reads, and is never a format-epoch mismatch.
    #[test]
    fn a_request_for_another_node_is_refused_with_the_refusing_nodes_id() {
        for target in [RaftTarget::Data(RaftGroupId(3)), RaftTarget::Meta] {
            for parse in requests_for(target, 8) {
                assert_eq!(parse(), Ok(target));
            }
            let request = stamped(pb::RejoinBarrierRequestV1::default(), target, 8);
            let status = tonic::Status::from(
                RaftTarget::parse(&request, 2).expect_err("node 2 is not node 8"),
            );
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert_eq!(refusing_node(&status), Some(2));
            assert!(!crate::format_epoch::is_protocol_mismatch(&status));
            assert!(crate::peer_cluster::refusing_cluster(&status).is_none());
            assert!(matches!(
                crate::format_epoch::classify_probe_answer(Err(status)),
                crate::PeerFormatEpoch::Unreachable(_)
            ));
        }
        // Other refusals name no node.
        assert_eq!(
            refusing_node(&tonic::Status::failed_precondition(
                "raft grpc protocol mismatch: local=6, remote=5"
            )),
            None
        );
    }

    #[test]
    fn a_target_admits_only_its_planes_commands() {
        let stream =
            crate::RaftCommand::Stream(ursula_stream::StreamCommand::AckColdGc { up_to_seq: 1 });
        let meta = crate::RaftCommand::Meta(crate::meta::MetaCommand::Control(
            ursula_control::ControlCommand::SetMoveLimit {
                limit: ursula_control::MoveLimit::new(2),
            },
        ));
        let data = RaftTarget::Data(RaftGroupId(4));
        assert!(data.admits(&stream));
        assert!(!data.admits(&meta));
        assert!(RaftTarget::Meta.admits(&meta));
        assert!(!RaftTarget::Meta.admits(&stream));
    }
}
