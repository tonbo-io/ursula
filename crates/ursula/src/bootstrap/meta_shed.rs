//! The meta plane's part in this node's leadership shed (design 3.2).
//!
//! - A node without a meta replica marks `PlacementUnavailable` (the meta
//!   runtime does), since a data group it led would never reconcile. It
//!   still campaigns but takes no transfer, and the drain here hands its
//!   data leaderships away, again every [`PLACEMENT_SHED_AGAIN`], as it may
//!   win elections meanwhile.
//! - A node that sheds for a reason that refuses transfers, the
//!   maintenance drain mark of `ursulactl drain` and of a shutdown
//!   included, hands its meta leadership away too, for as long as that
//!   reason holds. The data drain (`ShedDrain`) follows data group ids,
//!   where meta's group 0 would meet data group 0, so meta's one
//!   leadership is handed off here, to a meta voter whose node answered that
//!   its meta replica may take the leadership, by the registry's handoff
//!   check. A node that sheds only for cold
//!   health still takes transfers and keeps meta: when every node sheds
//!   for cold health (an object store outage), handing meta on would move
//!   it from voter to voter once per settle time, and its writes and
//!   barriers would keep finding no leader.
//!
//! Cold health, commit stall and egress read only the data groups'
//! metrics, so meta never makes a node shed.

use std::collections::HashMap;
use std::time::Duration;

use tokio::task::JoinHandle;
// `tokio::time::Instant` so the deadlines follow tokio's clock, which
// tests pause and madsim simulates.
use tokio::time::Instant;
use ursula_proto::admin::RaftTargetKey;
use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::meta::runtime::MetaRuntimeState;
use ursula_raft::meta::status::MetaStatusReader;

use crate::bootstrap::health_gate::LeadershipShed;
use crate::bootstrap::health_gate::ShedMark;
use crate::bootstrap::health_gate::ShedPace;
use crate::bootstrap::participation::CampaignStances;
use crate::bootstrap::participation::PeerParticipation;

/// How often the meta shed looks at the node's shed state and meta
/// leadership.
const META_SHED_TICK: Duration = Duration::from_secs(1);
/// How often a node without a meta replica starts its drain again.
const PLACEMENT_SHED_AGAIN: Duration = Duration::from_secs(30);

/// Runs the meta shed of node `node_id` (see the module docs) until the
/// returned task is aborted.
pub(crate) fn spawn_meta_shed(
    registry: RaftGroupHandleRegistry,
    participation: PeerParticipation,
    meta: MetaStatusReader,
    node_id: u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut placement = LeadershipShed::new(
            &registry,
            LeadershipShedReason::PlacementUnavailable,
            node_id,
            ShedPace::Gradual,
            ShedMark::UntilEnd,
        );
        let mut placement_began: Option<Instant> = None;
        let mut meta_settles: Option<Instant> = None;
        loop {
            tokio::time::sleep(META_SHED_TICK).await;
            let now = Instant::now();
            if matches!(meta.state(), MetaRuntimeState::Damaged { .. })
                && placement_began.is_none_or(|began| {
                    now.saturating_duration_since(began) >= PLACEMENT_SHED_AGAIN
                })
            {
                placement.begin();
                placement_began = Some(now);
            }
            let hand_off_meta = !registry.leadership_shed_state().should_accept_transfer()
                && leads_meta(&registry, node_id)
                && meta_settles.is_none_or(|settles| now >= settles);
            if !placement.is_draining() && !hand_off_meta {
                continue;
            }
            let stances = participation.campaign_stances(&registry).await;
            if placement.is_draining() {
                placement.step(&stances.shed_targets(), &[]).await;
            }
            if hand_off_meta
                && let Some(settle) = hand_off_meta_leadership(&registry, node_id, &stances).await
            {
                meta_settles = now.checked_add(settle);
            }
        }
    })
}

/// Whether node `node_id` leads the meta group now.
pub(crate) fn leads_meta(registry: &RaftGroupHandleRegistry, node_id: u64) -> bool {
    registry
        .meta_metrics_snapshot()
        .is_some_and(|meta| meta.current_leader == Some(node_id))
}

/// Hands node `node_id`'s meta leadership to the first meta voter whose
/// node answered that its meta replica may take the leadership
/// ([`CampaignStances::may_take`]) and that the registry's handoff check
/// accepts, and returns how long the handoff may take to move it. `None`
/// when this node does not lead meta or no voter took the handoff.
async fn hand_off_meta_leadership(
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    stances: &CampaignStances,
) -> Option<Duration> {
    let meta = registry.meta_metrics_snapshot()?;
    if meta.current_leader != Some(node_id) {
        return None;
    }
    // The meta group has no placement.
    let targets = crate::bootstrap::util::prioritized_transfer_targets(
        &meta,
        node_id,
        None,
        |node| stances.may_take(node, RaftTargetKey::Meta),
        &HashMap::new(),
    );
    for target in targets {
        if registry.check_meta_handoff(target).is_err() {
            continue;
        }
        match registry.transfer_meta_leader(target).await {
            Ok(()) => {
                tracing::warn!(
                    node_id,
                    target,
                    "leadership-shed: handing off the meta leadership"
                );
                return registry.meta_handoff_settle_time();
            }
            Err(err) => tracing::warn!(
                node_id,
                target,
                %err,
                "leadership-shed: the meta leadership handoff failed"
            ),
        }
    }
    None
}
