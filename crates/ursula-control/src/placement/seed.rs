//! The seed rule: where Bootstrap places each group, who initializes it, and
//! the default meta voters.
//!
//! Group `g` of a cluster of `N` nodes is seeded on `rf` consecutive nodes in
//! ascending id, starting at index `g mod N`, and initialized by the first of
//! them. So each node holds between `rf * floor(G / N)` and
//! `rf * ceil(G / N)` of `G` groups and initializes `floor(G / N)` or
//! `ceil(G / N)` of them. With `N <= rf` every node votes in every group.

use std::num::NonZeroU8;

use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeSet;
use ursula_proto::placement::ReplicationFactor;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;

/// How many meta voters a cluster has by default, at most.
const DEFAULT_META_VOTERS: usize = 3;

/// The index in `nodes` at which group `group`'s seed window starts.
fn rotation_start(group: RaftGroupId, nodes: &NodeSet) -> usize {
    usize::try_from(group.0)
        .ok()
        .and_then(|group| group.checked_rem(nodes.len()))
        .unwrap_or(0)
}

/// The node that initializes `group`: the first node of its seed window. It
/// is stored at Bootstrap, since the node set grows later.
pub fn initializer(group: RaftGroupId, nodes: &NodeSet) -> NodeId {
    nodes
        .iter()
        .nth(rotation_start(group, nodes))
        .unwrap_or(nodes.min())
}

/// The voters Bootstrap seeds `group` on: `min(rf, N)` consecutive nodes in
/// ascending id from index `group mod N`, wrapping around.
pub fn seed_voters(group: RaftGroupId, rf: ReplicationFactor, nodes: &NodeSet) -> VoterSet {
    let ordered: Vec<NodeId> = nodes.iter().collect();
    let window = ordered
        .iter()
        .copied()
        .cycle()
        .skip(rotation_start(group, nodes))
        .take(usize::from(rf.get()).min(nodes.len()));
    VoterSet::from_first(initializer(group, nodes), window)
}

/// The meta voters of a cluster that configures none: its smallest
/// `min(3, N)` node ids.
pub fn default_meta_voters(nodes: &NodeSet) -> VoterSet {
    VoterSet::from_first(nodes.min(), nodes.iter().take(DEFAULT_META_VOTERS))
}

/// The replication factor a cluster of `nodes` runs with: the configured
/// one, at most the node count.
pub fn effective_replication_factor(
    configured: ReplicationFactor,
    nodes: &NodeSet,
) -> ReplicationFactor {
    let count = u8::try_from(nodes.len()).unwrap_or(u8::MAX);
    match NonZeroU8::new(count) {
        Some(count) if count.get() < configured.get() => ReplicationFactor::new(count),
        Some(_) | None => configured,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::num::NonZeroU8;

    use proptest::prelude::ProptestConfig;
    use proptest::test_runner::RngSeed;
    use ursula_proto::placement::NodeId;
    use ursula_proto::placement::NodeSet;
    use ursula_proto::placement::ReplicationFactor;
    use ursula_shard::RaftGroupId;

    use super::default_meta_voters;
    use super::effective_replication_factor;
    use super::initializer;
    use super::seed_voters;

    fn rf(value: u8) -> ReplicationFactor {
        ReplicationFactor::new(NonZeroU8::new(value).unwrap())
    }

    fn node_set(ids: &BTreeSet<u64>) -> NodeSet {
        NodeSet::new(ids.iter().copied().map(NodeId::new).collect()).unwrap()
    }

    #[test]
    fn three_nodes_at_rf_3_vote_everywhere_and_rotate_the_initializer() {
        let nodes = node_set(&BTreeSet::from([1, 2, 3]));
        for group in 0..6 {
            let voters = seed_voters(RaftGroupId(group), rf(3), &nodes);
            assert_eq!(voters.to_set(), nodes.to_set());
        }
        let initializers: Vec<u64> = (0..6)
            .map(|group| initializer(RaftGroupId(group), &nodes).get())
            .collect();
        assert_eq!(initializers, vec![1, 2, 3, 1, 2, 3]);
    }

    #[test]
    fn a_window_wraps_around_the_largest_id() {
        let nodes = node_set(&BTreeSet::from([2, 4, 7, 9]));
        let voters = seed_voters(RaftGroupId(3), rf(3), &nodes);
        assert_eq!(
            voters.to_set(),
            BTreeSet::from([NodeId::new(9), NodeId::new(2), NodeId::new(4)])
        );
        assert_eq!(initializer(RaftGroupId(3), &nodes), NodeId::new(9));
    }

    #[test]
    fn the_effective_replication_factor_is_at_most_the_node_count() {
        let two = node_set(&BTreeSet::from([1, 2]));
        assert_eq!(effective_replication_factor(rf(3), &two), rf(2));
        assert_eq!(effective_replication_factor(rf(1), &two), rf(1));
    }

    proptest::proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            rng_seed: RngSeed::Fixed(768),
            // The seed is fixed, so a failure reproduces without a regression file.
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        /// S1 to S4.
        #[test]
        fn the_seed_rule_spreads_voters_and_initializers_evenly(
            ids in proptest::collection::btree_set(1_u64..64, 1..8),
            groups in 1_u32..64,
            configured in 1_u8..=3,
        ) {
            let nodes = node_set(&ids);
            let n = nodes.len();
            let effective = usize::from(effective_replication_factor(rf(configured), &nodes).get());
            let mut held: BTreeMap<NodeId, usize> = BTreeMap::new();
            let mut initialized: BTreeMap<NodeId, usize> = BTreeMap::new();
            for group in 0..groups {
                let voters = seed_voters(RaftGroupId(group), rf(configured), &nodes);
                // S1: `min(rf, N)` distinct voters, all of them nodes.
                proptest::prop_assert_eq!(voters.len(), effective);
                proptest::prop_assert!(voters.iter().all(|voter| nodes.contains(voter)));
                // S3: the initializer is a seed voter.
                let first = initializer(RaftGroupId(group), &nodes);
                proptest::prop_assert!(voters.contains(first));
                for voter in voters.iter() {
                    let count = held.entry(voter).or_default();
                    *count = count.checked_add(1).unwrap();
                }
                let count = initialized.entry(first).or_default();
                *count = count.checked_add(1).unwrap();
            }
            let total = usize::try_from(groups).unwrap();
            let floor = total.checked_div(n).unwrap();
            let ceil = if total.checked_rem(n).unwrap() == 0 { floor } else { floor.checked_add(1).unwrap() };
            for node in nodes.iter() {
                // S2: each node holds `rf * floor(G / N)` to `rf * ceil(G / N)` groups.
                let holds = held.get(&node).copied().unwrap_or(0);
                proptest::prop_assert!(holds >= effective.checked_mul(floor).unwrap());
                proptest::prop_assert!(holds <= effective.checked_mul(ceil).unwrap());
                // S3: each node initializes `floor(G / N)` or `ceil(G / N)` groups.
                let initializes = initialized.get(&node).copied().unwrap_or(0);
                proptest::prop_assert!(initializes == floor || initializes == ceil);
            }
            // S4: the default meta voters are the smallest `min(3, N)` ids.
            let expected: BTreeSet<NodeId> = nodes.iter().take(3).collect();
            proptest::prop_assert_eq!(default_meta_voters(&nodes).to_set(), expected);
        }
    }
}
