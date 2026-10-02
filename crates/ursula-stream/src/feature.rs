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
//!   streams, F3 producer-state bounds (a 1,024-item receipt window beyond
//!   each producer's newest receipt, duplicates beyond it deduplicated
//!   without ranges, 7-day idle-producer expiry), F4a message-record
//!   collapse at every cold transition, `TidyStream`, F18 step 2 cold
//!   coverage derived from the hot buffer (the scalar cold frontier is no
//!   longer read; snapshot field 6 carries the seal point), F14b
//!   `DeferColdGc`, F14i retention grace for dropped pack slices, the
//!   `FlushCold` incarnation check, and F12a MessagePack snapshot envelopes.
//!
//! - [`FEATURE_LEVEL_SPARSE_MARKS`] (2): bounded-state Lb2 — F1 sparse cold
//!   record marks. `FlushCold`, `AppendExternal`, external creates and
//!   `TidyStream` seal dense record offsets below the seal point into one
//!   mark per 1 MiB block (at most [`crate::SEAL_BUDGET_RECORDS`] per
//!   command), retention into sealed history lands on the mark at or below
//!   its target, and snapshots carry the marks (stream entry fields 17-19).
//!   Operators raise it only after every group completed a cold-index
//!   page-repair cycle (F19).
//!
//! - [`FEATURE_LEVEL_EXTERNAL_LOCATORS`] (3): bounded-state Lb3 (F5, Pi C6) —
//!   external payload locators are committed first and indexed after: an
//!   `AppendExternal` keeps its `ObjectPayloadRef` in replicated state at
//!   apply, the engine writes no cold-index page entry before proposing, and
//!   the leader's offload pass writes page entries for committed refs and
//!   then removes them from state with `OffloadColdRefs`. Raised, like
//!   level 2, only after a completed cold-index page-repair cycle.
//!
//! - [`FEATURE_LEVEL_HOT_REPRESENTATION`] (4): bounded-state Lb4 (F4b) —
//!   message records are removed from replicated state. Streams with a
//!   record index (JSON) take their message boundaries from the dense record
//!   offsets; other streams keep the start offset of every message at or
//!   above the seal point in the hot buffer (8 B per message). Bootstrap,
//!   snapshot alignment, retention and restore derive boundaries from those,
//!   snapshots carry the append starts (stream entry field 20) and no
//!   longer write message records (field 10), and a stream that still holds
//!   legacy message records converts them on its next write, flush,
//!   retention or `TidyStream`.
//!
//! Later core-track changes (C1, C3, C4, U22) take the remaining levels in
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

/// Bounded-state Lb2: F1 sparse cold record marks.
pub const FEATURE_LEVEL_SPARSE_MARKS: u32 = 2;

/// Bounded-state Lb3 (F5): external payload locators committed in state and
/// offloaded to cold-index pages by `OffloadColdRefs`.
pub const FEATURE_LEVEL_EXTERNAL_LOCATORS: u32 = 3;

/// Bounded-state Lb4 (F4b): message records removed; boundaries come from
/// the dense record offsets (JSON) or the hot buffer's append starts.
pub const FEATURE_LEVEL_HOT_REPRESENTATION: u32 = 4;

/// Highest group feature level this binary can apply.
pub const MAX_SUPPORTED_FEATURE_LEVEL: u32 = FEATURE_LEVEL_HOT_REPRESENTATION;

const _: () = assert!(MAX_SUPPORTED_FEATURE_LEVEL >= FEATURE_LEVEL_HOT_REPRESENTATION);

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
