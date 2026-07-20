use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamErrorContext;

use crate::engine::GroupEngine;
use crate::engine::GroupEngineError;
use crate::error::RuntimeError;
use crate::request::AppendBatchRequest;
use crate::request::ColdWriteAdmission;
use crate::rt::time::Instant;

pub(crate) const GROUP_ACTOR_MAX_WRITE_BATCH: usize = 64;
pub(crate) const COLD_FLUSH_GROUP_BATCH_MAX_CHUNKS: usize = 64;

#[derive(Debug, Clone)]
pub struct RuntimeMetrics {
    pub(crate) inner: Arc<RuntimeMetricsInner>,
}

/// Declares every runtime metric once and expands the four sections that were
/// previously hand-replicated per metric: the `RuntimeMetricsInner` counter
/// fields, `RuntimeMetricsInner::new`, the `RuntimeMetrics::snapshot`
/// collection logic, and the public serialized `RuntimeMetricsSnapshot`
/// struct.
///
/// Manifest grammar (entries must be listed in serialized snapshot-field
/// order; every counter and snapshot field name is spelled explicitly so
/// serialized names stay grep-able and byte-stable):
///
/// - `sum GLOBAL: core PER_CORE, group PER_GROUP;` — per-core and per-group
///   counters; the global value is the sum across cores.
/// - `sum GLOBAL: core PER_CORE;` — per-core counters summed into the global.
/// - `sum GLOBAL: group PER_GROUP;` — per-group counters summed into the
///   global.
/// - `max GLOBAL: group PER_GROUP;` — per-group values; the global is the
///   maximum.
/// - `summax SUM, MAX: group PER_GROUP;` — per-group values exposed both as a
///   sum and as a maximum.
/// - `counter GLOBAL;` — a single global counter.
macro_rules! runtime_metrics {
    (@munch
        ctx { $ir:ident $cc:ident $gc:ident }
        inner { $($inner:tt)* }
        new { $($new:tt)* }
        snap { $($snap:tt)* }
        fields { $($fields:tt)* }
        names { $($names:ident)* }
        rest { sum $global:ident: core $core:ident, group $group:ident; $($rest:tt)* }
    ) => {
        runtime_metrics! {
            @munch
            ctx { $ir $cc $gc }
            inner {
                $($inner)*
                pub(crate) $core: Vec<PaddedAtomicU64>,
                pub(crate) $group: Vec<PaddedAtomicU64>,
            }
            new {
                $($new)*
                $core: zeroed_counters($cc),
                $group: zeroed_counters($gc),
            }
            snap {
                $($snap)*
                let $core = load_counters(&$ir.$core);
                let $global: u64 = $core.iter().sum();
                let $group = load_counters(&$ir.$group);
            }
            fields {
                $($fields)*
                pub $global: u64,
                pub $core: Vec<u64>,
                pub $group: Vec<u64>,
            }
            names { $($names)* $global $core $group }
            rest { $($rest)* }
        }
    };
    (@munch
        ctx { $ir:ident $cc:ident $gc:ident }
        inner { $($inner:tt)* }
        new { $($new:tt)* }
        snap { $($snap:tt)* }
        fields { $($fields:tt)* }
        names { $($names:ident)* }
        rest { sum $global:ident: core $core:ident; $($rest:tt)* }
    ) => {
        runtime_metrics! {
            @munch
            ctx { $ir $cc $gc }
            inner {
                $($inner)*
                pub(crate) $core: Vec<PaddedAtomicU64>,
            }
            new {
                $($new)*
                $core: zeroed_counters($cc),
            }
            snap {
                $($snap)*
                let $core = load_counters(&$ir.$core);
                let $global: u64 = $core.iter().sum();
            }
            fields {
                $($fields)*
                pub $global: u64,
                pub $core: Vec<u64>,
            }
            names { $($names)* $global $core }
            rest { $($rest)* }
        }
    };
    (@munch
        ctx { $ir:ident $cc:ident $gc:ident }
        inner { $($inner:tt)* }
        new { $($new:tt)* }
        snap { $($snap:tt)* }
        fields { $($fields:tt)* }
        names { $($names:ident)* }
        rest { sum $global:ident: group $group:ident; $($rest:tt)* }
    ) => {
        runtime_metrics! {
            @munch
            ctx { $ir $cc $gc }
            inner {
                $($inner)*
                pub(crate) $group: Vec<PaddedAtomicU64>,
            }
            new {
                $($new)*
                $group: zeroed_counters($gc),
            }
            snap {
                $($snap)*
                let $group = load_counters(&$ir.$group);
                let $global: u64 = $group.iter().sum();
            }
            fields {
                $($fields)*
                pub $global: u64,
                pub $group: Vec<u64>,
            }
            names { $($names)* $global $group }
            rest { $($rest)* }
        }
    };
    (@munch
        ctx { $ir:ident $cc:ident $gc:ident }
        inner { $($inner:tt)* }
        new { $($new:tt)* }
        snap { $($snap:tt)* }
        fields { $($fields:tt)* }
        names { $($names:ident)* }
        rest { max $global:ident: group $group:ident; $($rest:tt)* }
    ) => {
        runtime_metrics! {
            @munch
            ctx { $ir $cc $gc }
            inner {
                $($inner)*
                pub(crate) $group: Vec<PaddedAtomicU64>,
            }
            new {
                $($new)*
                $group: zeroed_counters($gc),
            }
            snap {
                $($snap)*
                let $group = load_counters(&$ir.$group);
                let $global = max_or_zero(&$group);
            }
            fields {
                $($fields)*
                pub $global: u64,
                pub $group: Vec<u64>,
            }
            names { $($names)* $global $group }
            rest { $($rest)* }
        }
    };
    (@munch
        ctx { $ir:ident $cc:ident $gc:ident }
        inner { $($inner:tt)* }
        new { $($new:tt)* }
        snap { $($snap:tt)* }
        fields { $($fields:tt)* }
        names { $($names:ident)* }
        rest { summax $sum:ident, $max:ident: group $group:ident; $($rest:tt)* }
    ) => {
        runtime_metrics! {
            @munch
            ctx { $ir $cc $gc }
            inner {
                $($inner)*
                pub(crate) $group: Vec<PaddedAtomicU64>,
            }
            new {
                $($new)*
                $group: zeroed_counters($gc),
            }
            snap {
                $($snap)*
                let $group = load_counters(&$ir.$group);
                let $sum: u64 = $group.iter().sum();
                let $max = max_or_zero(&$group);
            }
            fields {
                $($fields)*
                pub $sum: u64,
                pub $max: u64,
                pub $group: Vec<u64>,
            }
            names { $($names)* $sum $max $group }
            rest { $($rest)* }
        }
    };
    (@munch
        ctx { $ir:ident $cc:ident $gc:ident }
        inner { $($inner:tt)* }
        new { $($new:tt)* }
        snap { $($snap:tt)* }
        fields { $($fields:tt)* }
        names { $($names:ident)* }
        rest { counter $global:ident; $($rest:tt)* }
    ) => {
        runtime_metrics! {
            @munch
            ctx { $ir $cc $gc }
            inner {
                $($inner)*
                pub(crate) $global: PaddedAtomicU64,
            }
            new {
                $($new)*
                $global: PaddedAtomicU64::new(0),
            }
            snap {
                $($snap)*
                let $global = $ir.$global.load_relaxed();
            }
            fields {
                $($fields)*
                pub $global: u64,
            }
            names { $($names)* $global }
            rest { $($rest)* }
        }
    };
    (@munch
        ctx { $ir:ident $cc:ident $gc:ident }
        inner { $($inner:tt)* }
        new { $($new:tt)* }
        snap { $($snap:tt)* }
        fields { $($fields:tt)* }
        names { $($names:ident)* }
        rest { }
    ) => {
        #[derive(Debug)]
        pub(crate) struct RuntimeMetricsInner {
            $($inner)*
        }

        impl RuntimeMetricsInner {
            pub(crate) fn new($cc: usize, $gc: usize) -> Self {
                Self { $($new)* }
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
        pub struct RuntimeMetricsSnapshot {
            $($fields)*
        }

        impl RuntimeMetrics {
            pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
                let $ir = &self.inner;
                $($snap)*
                RuntimeMetricsSnapshot { $($names),* }
            }
        }
    };
    ( $($manifest:tt)* ) => {
        runtime_metrics! {
            @munch
            ctx { inner_counters core_count raft_group_count }
            inner {}
            new {}
            snap {}
            fields {}
            names {}
            rest { $($manifest)* }
        }
    };
}

fn zeroed_counters(len: usize) -> Vec<PaddedAtomicU64> {
    (0..len).map(|_| PaddedAtomicU64::new(0)).collect()
}

fn load_counters(counters: &[PaddedAtomicU64]) -> Vec<u64> {
    counters.iter().map(PaddedAtomicU64::load_relaxed).collect()
}

fn max_or_zero(values: &[u64]) -> u64 {
    values.iter().copied().max().unwrap_or(0)
}

runtime_metrics! {
    sum accepted_appends: core per_core_appends, group per_group_appends;
    sum applied_mutations:
        core per_core_applied_mutations, group per_group_applied_mutations;
    sum mutation_apply_ns:
        core per_core_mutation_apply_ns, group per_group_mutation_apply_ns;
    sum group_lock_wait_ns:
        core per_core_group_lock_wait_ns, group per_group_group_lock_wait_ns;
    sum group_engine_exec_ns:
        core per_core_group_engine_exec_ns, group per_group_group_engine_exec_ns;
    sum group_mailbox_depth: group per_group_group_mailbox_depth;
    max group_mailbox_max_depth: group per_group_group_mailbox_max_depth;
    sum group_mailbox_full_events: group per_group_group_mailbox_full_events;
    sum raft_write_many_batches:
        core per_core_raft_write_many_batches, group per_group_raft_write_many_batches;
    sum raft_write_many_commands:
        core per_core_raft_write_many_commands, group per_group_raft_write_many_commands;
    sum raft_write_many_logical_commands:
        core per_core_raft_write_many_logical_commands,
        group per_group_raft_write_many_logical_commands;
    sum raft_write_many_responses:
        core per_core_raft_write_many_responses, group per_group_raft_write_many_responses;
    sum raft_write_many_submit_ns:
        core per_core_raft_write_many_submit_ns, group per_group_raft_write_many_submit_ns;
    sum raft_write_many_response_ns:
        core per_core_raft_write_many_response_ns, group per_group_raft_write_many_response_ns;
    sum raft_apply_entries: core per_core_raft_apply_entries, group per_group_raft_apply_entries;
    sum raft_apply_ns: core per_core_raft_apply_ns, group per_group_raft_apply_ns;
    sum raft_snapshot_builds: group per_group_raft_snapshot_builds;
    sum raft_snapshot_build_ns: group per_group_raft_snapshot_build_ns;
    summax raft_snapshot_body_bytes, raft_snapshot_body_bytes_max:
        group per_group_raft_snapshot_body_bytes;
    summax raft_snapshot_pointer_bytes, raft_snapshot_pointer_bytes_max:
        group per_group_raft_snapshot_pointer_bytes;
    summax raft_snapshot_streams, raft_snapshot_streams_max:
        group per_group_raft_snapshot_streams;
    sum raft_snapshot_external_uploads: group per_group_raft_snapshot_external_uploads;
    sum raft_snapshot_inline_fallbacks: group per_group_raft_snapshot_inline_fallbacks;
    sum live_read_waiters: core per_core_live_read_waiters;
    sum live_read_backpressure_events: core per_core_live_read_backpressure_events;
    sum routed_requests: core per_core_routed_requests;
    sum mailbox_send_wait_ns: core per_core_mailbox_send_wait_ns;
    sum mailbox_full_events: core per_core_mailbox_full_events;
    sum wal_batches: core per_core_wal_batches, group per_group_wal_batches;
    sum wal_records: core per_core_wal_records, group per_group_wal_records;
    sum wal_write_ns: core per_core_wal_write_ns, group per_group_wal_write_ns;
    sum wal_sync_ns: core per_core_wal_sync_ns, group per_group_wal_sync_ns;
    counter cold_flush_uploads;
    counter cold_flush_upload_bytes;
    counter cold_flush_upload_ns;
    counter cold_flush_publishes;
    counter cold_flush_publish_bytes;
    counter cold_flush_publish_ns;
    counter cold_orphan_cleanup_attempts;
    counter cold_orphan_cleanup_errors;
    counter cold_orphan_bytes;
    counter cold_gc_reclaimed;
    counter cold_gc_errors;
    counter cold_flush_write_errors;
    sum cold_hot_bytes: group per_group_cold_hot_bytes;
    max cold_hot_group_bytes_max: group per_group_cold_hot_bytes_max;
    counter cold_hot_stream_bytes_max;
    sum cold_backpressure_events:
        core per_core_cold_backpressure_events, group per_group_cold_backpressure_events;
    counter cold_backpressure_bytes;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMailboxSnapshot {
    pub depths: Vec<usize>,
    pub capacities: Vec<usize>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RaftWriteManySample {
    pub(crate) command_count: u64,
    pub(crate) logical_command_count: u64,
    pub(crate) response_count: u64,
    pub(crate) submit_ns: u64,
    pub(crate) response_ns: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RaftSnapshotBuildSample {
    pub(crate) streams: u64,
    pub(crate) body_bytes: u64,
    pub(crate) pointer_bytes: u64,
    pub(crate) build_ns: u64,
    pub(crate) external_upload: bool,
    pub(crate) inline_fallback: bool,
}

impl RuntimeMetricsInner {
    pub(crate) fn record_routed_request(&self, core_id: CoreId, mailbox_send_wait_ns: u64) {
        let index = usize::from(core_id.0);
        self.per_core_routed_requests[index].fetch_add_relaxed(1);
        self.per_core_mailbox_send_wait_ns[index].fetch_add_relaxed(mailbox_send_wait_ns);
    }

    pub(crate) fn record_mailbox_full(&self, core_id: CoreId) {
        self.per_core_mailbox_full_events[usize::from(core_id.0)].fetch_add_relaxed(1);
    }

    pub(crate) fn record_append(&self, core_id: CoreId, group_id: RaftGroupId) {
        self.record_append_batch(core_id, group_id, 1);
    }

    pub(crate) fn record_append_batch(&self, core_id: CoreId, group_id: RaftGroupId, count: u64) {
        self.per_core_appends[usize::from(core_id.0)].fetch_add_relaxed(count);
        self.per_group_appends[usize::try_from(group_id.0).expect("u32 fits usize")]
            .fetch_add_relaxed(count);
    }

    pub(crate) fn record_applied_mutation(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        apply_ns: u64,
    ) {
        self.record_applied_mutation_batch(core_id, group_id, 1, apply_ns);
    }

    pub(crate) fn record_applied_mutation_batch(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        count: u64,
        apply_ns: u64,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_applied_mutations[core_index].fetch_add_relaxed(count);
        self.per_group_applied_mutations[group_index].fetch_add_relaxed(count);
        self.per_core_mutation_apply_ns[core_index].fetch_add_relaxed(apply_ns);
        self.per_group_mutation_apply_ns[group_index].fetch_add_relaxed(apply_ns);
    }

    pub(crate) fn record_group_engine_exec(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        exec_ns: u64,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_group_engine_exec_ns[core_index].fetch_add_relaxed(exec_ns);
        self.per_group_group_engine_exec_ns[group_index].fetch_add_relaxed(exec_ns);
    }

    pub(crate) fn record_group_mailbox_enqueued(&self, group_id: RaftGroupId) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        let depth = self.per_group_group_mailbox_depth[group_index]
            .fetch_add_relaxed(1)
            .saturating_add(1);
        self.per_group_group_mailbox_max_depth[group_index].fetch_max_relaxed(depth);
    }

    pub(crate) fn record_group_mailbox_dequeued(&self, group_id: RaftGroupId) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_group_group_mailbox_depth[group_index].fetch_sub_saturating_relaxed(1);
    }

    pub(crate) fn record_group_mailbox_full(&self, group_id: RaftGroupId) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_group_group_mailbox_full_events[group_index].fetch_add_relaxed(1);
    }

    pub(crate) fn record_raft_write_many(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        sample: RaftWriteManySample,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_raft_write_many_batches[core_index].fetch_add_relaxed(1);
        self.per_group_raft_write_many_batches[group_index].fetch_add_relaxed(1);
        self.per_core_raft_write_many_commands[core_index].fetch_add_relaxed(sample.command_count);
        self.per_group_raft_write_many_commands[group_index]
            .fetch_add_relaxed(sample.command_count);
        self.per_core_raft_write_many_logical_commands[core_index]
            .fetch_add_relaxed(sample.logical_command_count);
        self.per_group_raft_write_many_logical_commands[group_index]
            .fetch_add_relaxed(sample.logical_command_count);
        self.per_core_raft_write_many_responses[core_index]
            .fetch_add_relaxed(sample.response_count);
        self.per_group_raft_write_many_responses[group_index]
            .fetch_add_relaxed(sample.response_count);
        self.per_core_raft_write_many_submit_ns[core_index].fetch_add_relaxed(sample.submit_ns);
        self.per_group_raft_write_many_submit_ns[group_index].fetch_add_relaxed(sample.submit_ns);
        self.per_core_raft_write_many_response_ns[core_index].fetch_add_relaxed(sample.response_ns);
        self.per_group_raft_write_many_response_ns[group_index]
            .fetch_add_relaxed(sample.response_ns);
    }

    pub(crate) fn record_raft_apply_batch(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        entry_count: u64,
        apply_ns: u64,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_raft_apply_entries[core_index].fetch_add_relaxed(entry_count);
        self.per_group_raft_apply_entries[group_index].fetch_add_relaxed(entry_count);
        self.per_core_raft_apply_ns[core_index].fetch_add_relaxed(apply_ns);
        self.per_group_raft_apply_ns[group_index].fetch_add_relaxed(apply_ns);
    }

    pub(crate) fn record_raft_snapshot_build(
        &self,
        group_id: RaftGroupId,
        sample: RaftSnapshotBuildSample,
    ) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_group_raft_snapshot_builds[group_index].fetch_add_relaxed(1);
        self.per_group_raft_snapshot_build_ns[group_index].fetch_add_relaxed(sample.build_ns);
        self.per_group_raft_snapshot_body_bytes[group_index].store_relaxed(sample.body_bytes);
        self.per_group_raft_snapshot_pointer_bytes[group_index].store_relaxed(sample.pointer_bytes);
        self.per_group_raft_snapshot_streams[group_index].store_relaxed(sample.streams);
        if sample.external_upload {
            self.per_group_raft_snapshot_external_uploads[group_index].fetch_add_relaxed(1);
        }
        if sample.inline_fallback {
            self.per_group_raft_snapshot_inline_fallbacks[group_index].fetch_add_relaxed(1);
        }
    }

    pub(crate) fn record_wal_batch(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        record_count: u64,
        write_ns: u64,
        sync_ns: u64,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_wal_batches[core_index].fetch_add_relaxed(1);
        self.per_group_wal_batches[group_index].fetch_add_relaxed(1);
        self.per_core_wal_records[core_index].fetch_add_relaxed(record_count);
        self.per_group_wal_records[group_index].fetch_add_relaxed(record_count);
        self.per_core_wal_write_ns[core_index].fetch_add_relaxed(write_ns);
        self.per_group_wal_write_ns[group_index].fetch_add_relaxed(write_ns);
        self.per_core_wal_sync_ns[core_index].fetch_add_relaxed(sync_ns);
        self.per_group_wal_sync_ns[group_index].fetch_add_relaxed(sync_ns);
    }

    pub(crate) fn record_cold_upload(&self, bytes: u64, upload_ns: u64) {
        self.cold_flush_uploads.fetch_add_relaxed(1);
        self.cold_flush_upload_bytes.fetch_add_relaxed(bytes);
        self.cold_flush_upload_ns.fetch_add_relaxed(upload_ns);
    }

    pub(crate) fn record_cold_publish(&self, bytes: u64, publish_ns: u64) {
        self.cold_flush_publishes.fetch_add_relaxed(1);
        self.cold_flush_publish_bytes.fetch_add_relaxed(bytes);
        self.cold_flush_publish_ns.fetch_add_relaxed(publish_ns);
    }

    pub(crate) fn record_cold_gc_reclaimed(&self, entries: u64) {
        self.cold_gc_reclaimed.fetch_add_relaxed(entries);
    }

    pub(crate) fn record_cold_flush_write_error(&self) {
        self.cold_flush_write_errors.fetch_add_relaxed(1);
    }

    pub(crate) fn record_cold_gc_error(&self) {
        self.cold_gc_errors.fetch_add_relaxed(1);
    }

    pub(crate) fn record_cold_hot_backlog(
        &self,
        group_id: RaftGroupId,
        stream_hot_bytes: u64,
        group_hot_bytes: u64,
    ) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_group_cold_hot_bytes[group_index].store_relaxed(group_hot_bytes);
        self.per_group_cold_hot_bytes_max[group_index].fetch_max_relaxed(group_hot_bytes);
        self.cold_hot_stream_bytes_max
            .fetch_max_relaxed(stream_hot_bytes);
    }

    pub(crate) fn record_cold_backpressure(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        incoming_bytes: u64,
        _limit: u64,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_cold_backpressure_events[core_index].fetch_add_relaxed(1);
        self.per_group_cold_backpressure_events[group_index].fetch_add_relaxed(1);
        self.cold_backpressure_bytes
            .fetch_add_relaxed(incoming_bytes);
    }

    pub(crate) fn record_read_watcher_added(&self, core_id: CoreId) {
        self.record_read_watchers_added(core_id, 1);
    }

    pub(crate) fn record_read_watchers_added(&self, core_id: CoreId, count: usize) {
        self.per_core_live_read_waiters[usize::from(core_id.0)]
            .fetch_add_relaxed(u64::try_from(count).expect("watcher count fits u64"));
    }

    pub(crate) fn record_read_watchers_removed(&self, core_id: CoreId, count: usize) {
        self.per_core_live_read_waiters[usize::from(core_id.0)]
            .fetch_sub_relaxed(u64::try_from(count).expect("watcher count fits u64"));
    }

    pub(crate) fn record_live_read_backpressure(&self, core_id: CoreId) {
        self.per_core_live_read_backpressure_events[usize::from(core_id.0)].fetch_add_relaxed(1);
    }
}

pub(crate) fn elapsed_ns(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn append_batch_payload_bytes(request: &AppendBatchRequest) -> u64 {
    request
        .payloads
        .iter()
        .map(|payload| u64::try_from(payload.len()).expect("payload len fits u64"))
        .sum()
}

pub(crate) fn record_cold_backpressure_error(
    metrics: &RuntimeMetricsInner,
    placement: ShardPlacement,
    incoming_bytes: u64,
    admission: ColdWriteAdmission,
    err: &GroupEngineError,
) {
    if !err.is_cold_backpressure() {
        return;
    }
    metrics.record_cold_backpressure(
        placement.core_id,
        placement.raft_group_id,
        incoming_bytes,
        admission.max_hot_bytes_per_group.unwrap_or(0),
    );
}

pub(crate) fn is_stale_cold_flush_candidate_error(err: &RuntimeError) -> bool {
    match err.stream_error_code() {
        Some(StreamErrorCode::StreamGone | StreamErrorCode::StreamNotFound) => true,
        Some(StreamErrorCode::InvalidColdFlush) => err
            .stream_error_context()
            .iter()
            .any(|context| matches!(context, StreamErrorContext::StaleColdFlushCandidate)),
        _ => false,
    }
}

pub(crate) async fn record_cold_hot_backlog(
    group: &mut Box<dyn GroupEngine>,
    metrics: &RuntimeMetricsInner,
    stream_id: BucketStreamId,
    placement: ShardPlacement,
) {
    if let Ok(backlog) = group.cold_hot_backlog(stream_id, placement).await {
        metrics.record_cold_hot_backlog(
            placement.raft_group_id,
            backlog.stream_hot_bytes,
            backlog.group_hot_bytes,
        );
    }
}

#[derive(Debug)]
#[repr(align(128))]
pub(crate) struct PaddedAtomicU64 {
    value: AtomicU64,
}

impl PaddedAtomicU64 {
    pub(crate) fn new(value: u64) -> Self {
        Self {
            value: AtomicU64::new(value),
        }
    }

    pub(crate) fn load_relaxed(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub(crate) fn fetch_add_relaxed(&self, value: u64) -> u64 {
        self.value.fetch_add(value, Ordering::Relaxed)
    }

    pub(crate) fn fetch_sub_relaxed(&self, value: u64) {
        self.value.fetch_sub(value, Ordering::Relaxed);
    }

    pub(crate) fn fetch_sub_saturating_relaxed(&self, value: u64) {
        let mut current = self.value.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(value);
            match self.value.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn fetch_max_relaxed(&self, value: u64) {
        self.value.fetch_max(value, Ordering::Relaxed);
    }

    pub(crate) fn store_relaxed(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod metric_manifest_tests {
    use std::sync::Arc;

    use crate::metrics::RuntimeMetrics;
    use crate::metrics::RuntimeMetricsInner;

    /// The serialized field names of [`RuntimeMetricsSnapshot`] in declaration
    /// order, captured from the pre-macro hand-written struct. Metrics
    /// endpoints and `ursulactl` depend on these names staying byte-identical.
    const EXPECTED_SNAPSHOT_KEYS: [&str; 105] = [
        "accepted_appends",
        "per_core_appends",
        "per_group_appends",
        "applied_mutations",
        "per_core_applied_mutations",
        "per_group_applied_mutations",
        "mutation_apply_ns",
        "per_core_mutation_apply_ns",
        "per_group_mutation_apply_ns",
        "group_lock_wait_ns",
        "per_core_group_lock_wait_ns",
        "per_group_group_lock_wait_ns",
        "group_engine_exec_ns",
        "per_core_group_engine_exec_ns",
        "per_group_group_engine_exec_ns",
        "group_mailbox_depth",
        "per_group_group_mailbox_depth",
        "group_mailbox_max_depth",
        "per_group_group_mailbox_max_depth",
        "group_mailbox_full_events",
        "per_group_group_mailbox_full_events",
        "raft_write_many_batches",
        "per_core_raft_write_many_batches",
        "per_group_raft_write_many_batches",
        "raft_write_many_commands",
        "per_core_raft_write_many_commands",
        "per_group_raft_write_many_commands",
        "raft_write_many_logical_commands",
        "per_core_raft_write_many_logical_commands",
        "per_group_raft_write_many_logical_commands",
        "raft_write_many_responses",
        "per_core_raft_write_many_responses",
        "per_group_raft_write_many_responses",
        "raft_write_many_submit_ns",
        "per_core_raft_write_many_submit_ns",
        "per_group_raft_write_many_submit_ns",
        "raft_write_many_response_ns",
        "per_core_raft_write_many_response_ns",
        "per_group_raft_write_many_response_ns",
        "raft_apply_entries",
        "per_core_raft_apply_entries",
        "per_group_raft_apply_entries",
        "raft_apply_ns",
        "per_core_raft_apply_ns",
        "per_group_raft_apply_ns",
        "raft_snapshot_builds",
        "per_group_raft_snapshot_builds",
        "raft_snapshot_build_ns",
        "per_group_raft_snapshot_build_ns",
        "raft_snapshot_body_bytes",
        "raft_snapshot_body_bytes_max",
        "per_group_raft_snapshot_body_bytes",
        "raft_snapshot_pointer_bytes",
        "raft_snapshot_pointer_bytes_max",
        "per_group_raft_snapshot_pointer_bytes",
        "raft_snapshot_streams",
        "raft_snapshot_streams_max",
        "per_group_raft_snapshot_streams",
        "raft_snapshot_external_uploads",
        "per_group_raft_snapshot_external_uploads",
        "raft_snapshot_inline_fallbacks",
        "per_group_raft_snapshot_inline_fallbacks",
        "live_read_waiters",
        "per_core_live_read_waiters",
        "live_read_backpressure_events",
        "per_core_live_read_backpressure_events",
        "routed_requests",
        "per_core_routed_requests",
        "mailbox_send_wait_ns",
        "per_core_mailbox_send_wait_ns",
        "mailbox_full_events",
        "per_core_mailbox_full_events",
        "wal_batches",
        "per_core_wal_batches",
        "per_group_wal_batches",
        "wal_records",
        "per_core_wal_records",
        "per_group_wal_records",
        "wal_write_ns",
        "per_core_wal_write_ns",
        "per_group_wal_write_ns",
        "wal_sync_ns",
        "per_core_wal_sync_ns",
        "per_group_wal_sync_ns",
        "cold_flush_uploads",
        "cold_flush_upload_bytes",
        "cold_flush_upload_ns",
        "cold_flush_publishes",
        "cold_flush_publish_bytes",
        "cold_flush_publish_ns",
        "cold_orphan_cleanup_attempts",
        "cold_orphan_cleanup_errors",
        "cold_orphan_bytes",
        "cold_gc_reclaimed",
        "cold_gc_errors",
        "cold_flush_write_errors",
        "cold_hot_bytes",
        "per_group_cold_hot_bytes",
        "cold_hot_group_bytes_max",
        "per_group_cold_hot_bytes_max",
        "cold_hot_stream_bytes_max",
        "cold_backpressure_events",
        "per_core_cold_backpressure_events",
        "per_group_cold_backpressure_events",
        "cold_backpressure_bytes",
    ];

    fn metrics_for_test() -> RuntimeMetrics {
        RuntimeMetrics {
            inner: Arc::new(RuntimeMetricsInner::new(2, 3)),
        }
    }

    #[test]
    fn snapshot_serializes_expected_field_names_in_order() {
        let json =
            serde_json::to_string(&metrics_for_test().snapshot()).expect("snapshot serializes");
        // Every value is a number or an array of numbers, so each `":`
        // occurrence in the output belongs to exactly one field key.
        assert_eq!(
            json.matches("\":").count(),
            EXPECTED_SNAPSHOT_KEYS.len(),
            "unexpected number of serialized fields: {json}"
        );
        let mut last_position = None;
        for name in EXPECTED_SNAPSHOT_KEYS {
            let needle = format!("\"{name}\":");
            let position = json
                .find(&needle)
                .unwrap_or_else(|| panic!("missing serialized key {name}"));
            assert!(
                last_position < Some(position),
                "serialized key {name} out of declaration order"
            );
            last_position = Some(position);
        }
    }

    #[test]
    fn snapshot_vector_lengths_follow_metric_scope() {
        let snapshot = metrics_for_test().snapshot();
        assert_eq!(snapshot.per_core_appends.len(), 2);
        assert_eq!(snapshot.per_group_appends.len(), 3);
        assert_eq!(snapshot.per_core_routed_requests.len(), 2);
        assert_eq!(snapshot.per_group_raft_snapshot_streams.len(), 3);
    }

    #[test]
    fn snapshot_aggregates_sum_and_max_per_manifest() {
        let metrics = metrics_for_test();
        metrics.inner.per_core_appends[0].fetch_add_relaxed(3);
        metrics.inner.per_core_appends[1].fetch_add_relaxed(4);
        metrics.inner.per_group_group_mailbox_max_depth[1].fetch_max_relaxed(9);
        metrics.inner.per_group_raft_snapshot_body_bytes[0].store_relaxed(5);
        metrics.inner.per_group_raft_snapshot_body_bytes[2].store_relaxed(11);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.accepted_appends, 7);
        assert_eq!(snapshot.group_mailbox_max_depth, 9);
        assert_eq!(snapshot.raft_snapshot_body_bytes, 16);
        assert_eq!(snapshot.raft_snapshot_body_bytes_max, 11);
    }
}
