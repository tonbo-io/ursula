//! Complete ordered placement projections; losing deltas needs no special repair.

use serde::Deserialize;
use serde::Serialize;

use crate::ClusterIdentity;
use crate::ControlPlaneState;
use crate::MembershipLogId;

/// A complete applied control snapshot from a quorum-confirmed meta leader.
/// The applied meta log is its version, including entries that change no
/// placement. Consumers replace the whole projection atomically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlProjection {
    pub identity: ClusterIdentity,
    pub applied_log_id: MembershipLogId,
    pub state: ControlPlaneState,
}

impl ControlProjection {
    /// A quorum-confirmed state may precede adoption. Only a completely empty
    /// control state is valid in that phase; established states need a full view.
    pub fn validate_bootstrap_state(&self) -> Result<(), String> {
        if self.state.cluster_bootstrap.is_some() {
            return self.validate();
        }
        self.identity.validate()?;
        if self.applied_log_id.node_id == 0
            || !self.state.nodes.is_empty()
            || !self.state.placements.is_empty()
            || !self.state.migrations.is_empty()
            || self.state.active_migration.is_some()
            || self.state.managed_placement.is_some()
            || self.state.next_migration_id != 1
        {
            return Err("pre-adoption state must be completely empty".to_owned());
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        self.identity.validate()?;
        if self.applied_log_id.node_id == 0 {
            return Err("projection applied log has no leader identity".to_owned());
        }
        let record = self
            .state
            .cluster_bootstrap
            .as_ref()
            .ok_or_else(|| "projection requires a persisted cluster bootstrap".to_owned())?;
        if record.recipe.identity != self.identity {
            return Err("projection differs from its persisted routing identity".to_owned());
        }
        let managed = self
            .state
            .managed_placement
            .as_ref()
            .ok_or_else(|| "projection requires persisted replication policies".to_owned())?;
        if managed.group_count != self.identity.group_count
            || self.state.placements.len() != self.identity.group_count as usize
            || managed.groups.len() != self.state.placements.len()
        {
            return Err("projection must include every configured group and policy".to_owned());
        }
        for raw_group in 0..self.identity.group_count {
            let group = ursula_shard::RaftGroupId(raw_group);
            let placement = self
                .state
                .placements
                .get(&group)
                .ok_or_else(|| format!("projection lacks group {raw_group}"))?;
            let policy = managed
                .groups
                .get(&group)
                .ok_or_else(|| format!("projection lacks policy for group {raw_group}"))?;
            if placement.raft_group_id != group {
                return Err(format!("invalid placement identity for group {raw_group}"));
            }
            policy.validate_voters(&placement.voters, &self.state.nodes)?;
            if !placement.voters.is_disjoint(&placement.learners)
                || placement
                    .learners
                    .iter()
                    .chain(placement.draining.iter())
                    .any(|id| !self.state.nodes.contains_key(id))
            {
                return Err(format!(
                    "invalid projection participants for group {raw_group}"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionInstall {
    Advanced,
    Unchanged,
    Stale,
}

/// Pure ordering/identity gate. The runtime owns synchronization and durable
/// local caching; this cursor cannot turn an old full snapshot into a rollback.
#[derive(Debug, Clone)]
pub struct ProjectionCursor {
    identity: ClusterIdentity,
    current: Option<ControlProjection>,
}

impl ProjectionCursor {
    pub fn new(identity: ClusterIdentity) -> Result<Self, String> {
        identity.validate()?;
        Ok(Self {
            identity,
            current: None,
        })
    }

    pub fn current(&self) -> Option<&ControlProjection> {
        self.current.as_ref()
    }

    pub fn install(&mut self, incoming: ControlProjection) -> Result<ProjectionInstall, String> {
        if incoming.identity != self.identity {
            return Err("projection routing identity differs from the local contract".to_owned());
        }
        incoming.validate()?;
        if let Some(current) = &self.current {
            if incoming.applied_log_id.index < current.applied_log_id.index {
                return Ok(ProjectionInstall::Stale);
            }
            if incoming.applied_log_id.index == current.applied_log_id.index {
                return if incoming == *current {
                    Ok(ProjectionInstall::Unchanged)
                } else {
                    Err("conflicting projections at the same applied index".to_owned())
                };
            }
            if incoming.applied_log_id.term < current.applied_log_id.term {
                return Err("projection term regressed at a newer applied index".to_owned());
            }
        }
        self.current = Some(incoming);
        Ok(ProjectionInstall::Advanced)
    }
}
