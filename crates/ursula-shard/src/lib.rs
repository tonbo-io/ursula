use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CoreId(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct RaftGroupId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketStreamId {
    pub bucket_id: String,
    pub stream_id: String,
}

impl BucketStreamId {
    pub fn new(bucket_id: impl Into<String>, stream_id: impl Into<String>) -> Self {
        Self {
            bucket_id: bucket_id.into(),
            stream_id: stream_id.into(),
        }
    }
}

impl From<BucketStreamId> for ursula_proto::BucketStreamIdV1 {
    fn from(stream_id: BucketStreamId) -> Self {
        Self {
            bucket_id: stream_id.bucket_id,
            stream_id: stream_id.stream_id,
        }
    }
}

impl From<&BucketStreamId> for ursula_proto::BucketStreamIdV1 {
    fn from(stream_id: &BucketStreamId) -> Self {
        Self {
            bucket_id: stream_id.bucket_id.clone(),
            stream_id: stream_id.stream_id.clone(),
        }
    }
}

impl From<ursula_proto::BucketStreamIdV1> for BucketStreamId {
    fn from(stream_id: ursula_proto::BucketStreamIdV1) -> Self {
        Self {
            bucket_id: stream_id.bucket_id,
            stream_id: stream_id.stream_id,
        }
    }
}

impl fmt::Display for BucketStreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.bucket_id, self.stream_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPlacement {
    pub core_id: CoreId,
    pub shard_id: ShardId,
    pub raft_group_id: RaftGroupId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardMapError {
    ZeroCores,
    ZeroRaftGroups,
    TooManyCores(usize),
    TooManyRaftGroups(usize),
}

impl fmt::Display for ShardMapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCores => f.write_str("core_count must be greater than zero"),
            Self::ZeroRaftGroups => f.write_str("raft_group_count must be greater than zero"),
            Self::TooManyCores(value) => write!(f, "core_count {value} does not fit into u16"),
            Self::TooManyRaftGroups(value) => {
                write!(f, "raft_group_count {value} does not fit into u32")
            }
        }
    }
}

impl std::error::Error for ShardMapError {}

#[derive(Debug, Clone)]
pub struct StaticShardMap {
    core_count: u16,
    raft_group_count: u32,
}

impl StaticShardMap {
    pub fn new(core_count: usize, raft_group_count: usize) -> Result<Self, ShardMapError> {
        if core_count == 0 {
            return Err(ShardMapError::ZeroCores);
        }
        if raft_group_count == 0 {
            return Err(ShardMapError::ZeroRaftGroups);
        }
        let core_count =
            u16::try_from(core_count).map_err(|_| ShardMapError::TooManyCores(core_count))?;
        let raft_group_count = u32::try_from(raft_group_count)
            .map_err(|_| ShardMapError::TooManyRaftGroups(raft_group_count))?;
        Ok(Self {
            core_count,
            raft_group_count,
        })
    }

    pub fn core_count(&self) -> u16 {
        self.core_count
    }

    pub fn raft_group_count(&self) -> u32 {
        self.raft_group_count
    }

    pub fn locate(&self, stream_id: &BucketStreamId) -> ShardPlacement {
        let hash = fnv1a64_stream_id(stream_id);
        let raft_group = (hash % u64::from(self.raft_group_count)) as u32;
        let core = (u64::from(raft_group) % u64::from(self.core_count)) as u16;
        ShardPlacement {
            core_id: CoreId(core),
            shard_id: ShardId(raft_group),
            raft_group_id: RaftGroupId(raft_group),
        }
    }
}

fn fnv1a64_stream_id(stream_id: &BucketStreamId) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for byte in stream_id.bucket_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash ^= u64::from(b'/');
    hash = hash.wrapping_mul(PRIME);
    for byte in stream_id.stream_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_dimensions() {
        assert!(matches!(
            StaticShardMap::new(0, 16),
            Err(ShardMapError::ZeroCores)
        ));
        assert!(matches!(
            StaticShardMap::new(4, 0),
            Err(ShardMapError::ZeroRaftGroups)
        ));
    }

    #[test]
    fn placement_is_stable_for_same_stream() {
        let map = StaticShardMap::new(4, 64).expect("valid shard map");
        let stream = BucketStreamId::new("agents", "session-42");
        assert_eq!(map.locate(&stream), map.locate(&stream));
    }

    #[test]
    fn bucket_stream_id_round_trips_through_shared_proto() {
        let stream = BucketStreamId::new("agents", "session-42");
        let proto = ursula_proto::BucketStreamIdV1::from(&stream);

        assert_eq!(proto.bucket_id, "agents");
        assert_eq!(proto.stream_id, "session-42");
        assert_eq!(BucketStreamId::from(proto), stream);
    }

    #[test]
    fn raft_group_is_owned_by_one_core() {
        let map = StaticShardMap::new(4, 64).expect("valid shard map");
        for group in 0..map.raft_group_count() {
            let core = group % u32::from(map.core_count());
            assert_eq!(core, group % 4);
        }
    }

    #[test]
    fn placement_stays_in_range() {
        let map = StaticShardMap::new(3, 17).expect("valid shard map");
        for index in 0..10_000 {
            let stream = BucketStreamId::new("b", format!("s-{index}"));
            let placement = map.locate(&stream);
            assert!(placement.core_id.0 < map.core_count());
            assert!(placement.raft_group_id.0 < map.raft_group_count());
        }
    }

    #[test]
    fn many_streams_reach_every_core() {
        let map = StaticShardMap::new(8, 128).expect("valid shard map");
        let mut seen = vec![false; usize::from(map.core_count())];
        for index in 0..10_000 {
            let stream = BucketStreamId::new("benchcmp", format!("stream-{index}"));
            let placement = map.locate(&stream);
            seen[usize::from(placement.core_id.0)] = true;
        }
        assert!(seen.into_iter().all(|value| value));
    }

    #[test]
    fn changing_raft_group_count_remaps_many_streams() {
        let old = StaticShardMap::new(8, 64).expect("valid old shard map");
        let new = StaticShardMap::new(8, 128).expect("valid new shard map");
        let mut remapped = 0usize;
        for index in 0..10_000 {
            let stream = BucketStreamId::new("benchcmp", format!("stream-{index}"));
            if old.locate(&stream).raft_group_id != new.locate(&stream).raft_group_id {
                remapped += 1;
            }
        }
        assert!(
            remapped > 4_000,
            "static modulo placement should be treated as non-incremental; remapped={remapped}"
        );
    }
}
