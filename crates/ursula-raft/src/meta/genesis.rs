//! The meta group's genesis probe: what one node asks another before the
//! meta group's first leader initializes it, and what it is answered.
//!
//! The probe travels on its own RPC (`GenesisProbe`), so the vote envelope
//! and its ack keep their shape. Its request is a [`GenesisProbe`] in
//! `GenesisProbeRequestV1.probe` and its answer a [`GenesisAnswer`] in
//! `GenesisProbeAckV1.answer`, each one MessagePack value in a required
//! field. Both are format (format epoch 6).

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::placement::GenesisClaim;

use crate::codec::WireDecodeError;
use crate::codec::decode_wire;
use crate::codec::encode_wire;
use crate::raft_internal_proto as pb;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

/// A node's genesis probe: the values it would start the meta group with,
/// and its probe vote, the `(0, 0)` vote a node answers from its meta
/// replica's recovery gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenesisProbe {
    pub claim: GenesisClaim,
    pub vote: UrsulaVoteRequest,
}

/// A node's answer to a genesis probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenesisAnswer {
    /// The node claims what the prober claims: its meta replica's answer to
    /// the probe vote.
    Vote(UrsulaVoteResponse),
    /// The node claims other values, which it names: the meta group must not
    /// form until every node claims the same.
    Mismatch(GenesisClaim),
}

/// What a genesis answer says of the meta group on the node that gave it,
/// as the bootstrap decision and the genesis vote floor read it.
impl From<GenesisAnswer> for crate::rejoin::PeerGroupLog {
    fn from(answer: GenesisAnswer) -> Self {
        match answer {
            GenesisAnswer::Vote(response) => Self::from_vote_response(&response),
            GenesisAnswer::Mismatch(claim) => Self::GenesisMismatch { claim },
        }
    }
}

/// The `GenesisProbe` request for `target_node` carrying `probe`.
pub(crate) fn genesis_probe_request(
    target_node: u64,
    probe: &GenesisProbe,
) -> pb::GenesisProbeRequestV1 {
    pb::GenesisProbeRequestV1 {
        protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
        target_node,
        probe: encode_wire(probe),
    }
}

/// The answer a `GenesisProbe` ack carries. An empty answer does not
/// decode: it is a malformed ack, never an answer.
pub(crate) fn genesis_answer(
    ack: &pb::GenesisProbeAckV1,
) -> Result<GenesisAnswer, WireDecodeError> {
    decode_wire(&ack.answer, "genesis answer")
}

#[cfg(test)]
mod tests {
    use ursula_control::samples;

    use super::GenesisAnswer;
    use super::GenesisProbe;
    use super::genesis_answer;
    use super::genesis_probe_request;
    use crate::codec::decode_wire;
    use crate::codec::encode_wire;
    use crate::raft_internal_proto as pb;

    fn probe() -> GenesisProbe {
        GenesisProbe {
            claim: samples::sample_genesis_claim(),
            vote: crate::rejoin::bootstrap_probe_vote(),
        }
    }

    #[test]
    fn a_genesis_probe_round_trips_through_its_request() {
        let request = genesis_probe_request(33, &probe());
        assert_eq!(
            request.protocol_version,
            crate::grpc::RAFT_GRPC_PROTOCOL_VERSION
        );
        assert_eq!(request.target_node, 33);
        let decoded: GenesisProbe = decode_wire(&request.probe, "probe").expect("decodes");
        assert_eq!(decoded, probe());
    }

    #[test]
    fn an_empty_genesis_answer_is_malformed() {
        let empty = pb::GenesisProbeAckV1 {
            answer: Vec::new().into(),
        };
        genesis_answer(&empty).expect_err("an empty answer does not decode");
        let mismatch = GenesisAnswer::Mismatch(samples::sample_genesis_claim());
        let ack = pb::GenesisProbeAckV1 {
            answer: encode_wire(&mismatch),
        };
        assert_eq!(genesis_answer(&ack).expect("decodes"), mismatch);
    }
}
