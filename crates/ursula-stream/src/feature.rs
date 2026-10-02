//! Replicated group feature levels (design C0, keyed-streams §6.2–§6.4).
//!
//! Every Raft group carries a replicated `u32` feature level, raised only by
//! [`StreamCommand::SetFeatureLevel`] and never lowered. A release that adds
//! apply-time behavior (a new replicated field, a new command, or a changed
//! apply rule) assigns that behavior to the next level and activates it only
//! once the group's level reaches it. Binaries from different releases
//! therefore never apply the same command differently: until an operator
//! raises the level — after every voter and learner runs a binary whose
//! [`MAX_SUPPORTED_FEATURE_LEVEL`] covers it (`ursulactl cluster
//! enable-feature`) — new binaries apply exactly what old ones would.
//!
//! Levels:
//!
//! - [`FEATURE_LEVEL_BASELINE`] (0): behavior of every release before C0.
//!   Snapshots and groups without a recorded level are at 0.
//! - [`FEATURE_LEVEL_KEYED_STREAMS`] (1): keyed-streams milestone M1 and
//!   bounded-state Lb1 — C7 unique stream incarnation (`created_at_ms`
//!   strictly increases per group), C8 apply-time reservation of the
//!   `keyed-state` stream name, F14a/F14g incarnation-scoped cold objects and
//!   stream GC, creation of `application/json; profile=keyed-batch-v1`
//!   streams, F18 step 2 cold coverage derived from the hot buffer (the
//!   scalar cold frontier is no longer read; snapshot field 6 carries the
//!   seal point), F14b `DeferColdGc`, F14i retention grace for dropped pack
//!   slices, the `FlushCold` incarnation check, and F12a MessagePack
//!   snapshot envelopes.
//!
//! Later core-track changes (C1, C3, C4, C6, U22) take the next levels in
//! release order.
//!
//! No downgrade: once a group's level is raised, a binary whose
//! [`MAX_SUPPORTED_FEATURE_LEVEL`] is lower must not run it. Snapshots record
//! their level and restoring one above this binary's supported level fails
//! with [`StreamSnapshotError::UnsupportedFeatureLevel`].
//!
//! [`StreamCommand::SetFeatureLevel`]: crate::StreamCommand::SetFeatureLevel
//! [`StreamSnapshotError::UnsupportedFeatureLevel`]: crate::StreamSnapshotError::UnsupportedFeatureLevel

/// Level of every group before any feature level is set.
pub const FEATURE_LEVEL_BASELINE: u32 = 0;

/// Keyed-streams M1: C7 unique incarnation, C8 apply-time `keyed-state`
/// reservation, and `keyed-batch-v1` stream creation.
pub const FEATURE_LEVEL_KEYED_STREAMS: u32 = 1;

/// Highest group feature level this binary can apply.
pub const MAX_SUPPORTED_FEATURE_LEVEL: u32 = FEATURE_LEVEL_KEYED_STREAMS;

const _: () = assert!(MAX_SUPPORTED_FEATURE_LEVEL >= FEATURE_LEVEL_KEYED_STREAMS);

/// Pure form of the apply-time gate: `Ok` when a group at `current` may run
/// an operation that needs `required`, otherwise the plain-text reason that
/// accompanies [`StreamErrorCode::FeatureNotEnabled`].
///
/// [`StreamErrorCode::FeatureNotEnabled`]: crate::StreamErrorCode::FeatureNotEnabled
pub fn check_feature_level(current: u32, required: u32, operation: &str) -> Result<(), String> {
    if current >= required {
        return Ok(());
    }
    Err(format!(
        "{operation} requires group feature level {required}; this group is at level {current} \
         (raise it with `ursulactl cluster enable-feature --level {required}` once every node \
         supports it)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_passes_at_or_above_required_level() {
        assert_eq!(check_feature_level(1, 1, "op"), Ok(()));
        assert_eq!(check_feature_level(2, 1, "op"), Ok(()));
        assert_eq!(check_feature_level(0, 0, "op"), Ok(()));
    }

    #[test]
    fn gate_names_required_and_current_level() {
        let Err(message) = check_feature_level(0, 1, "keyed stream create") else {
            panic!("level 0 must not satisfy level 1");
        };
        assert!(message.starts_with("keyed stream create requires group feature level 1"));
        assert!(message.contains("at level 0"));
    }
}
