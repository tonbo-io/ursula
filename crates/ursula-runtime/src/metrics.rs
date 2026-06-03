use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::rt::time::Instant;

use ursula_shard::{BucketStreamId, CoreId, RaftGroupId, ShardPlacement};

use crate::engine::{GroupEngine, GroupEngineError};
use crate::error::RuntimeError;
use crate::request::{AppendBatchRequest, ColdWriteAdmission};

pub(crate) const GROUP_ACTOR_MAX_WRITE_BATCH: usize = 64;
pub(crate) const COLD_FLUSH_GROUP_BATCH_MAX_CHUNKS: usize = 64;

#[derive(Debug, Clone)]
pub struct RuntimeMetrics {
    pub(crate) inner: Arc<RuntimeMetricsInner>,
}

impl RuntimeMetrics {
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        let per_core_appends = self
            .inner
            .per_core_appends
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let accepted_appends = per_core_appends.iter().sum();
        let per_group_appends = self
            .inner
            .per_group_appends
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_applied_mutations = self
            .inner
            .per_core_applied_mutations
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let applied_mutations = per_core_applied_mutations.iter().sum();
        let per_group_applied_mutations = self
            .inner
            .per_group_applied_mutations
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_mutation_apply_ns = self
            .inner
            .per_core_mutation_apply_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let mutation_apply_ns = per_core_mutation_apply_ns.iter().sum();
        let per_group_mutation_apply_ns = self
            .inner
            .per_group_mutation_apply_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_group_lock_wait_ns = self
            .inner
            .per_core_group_lock_wait_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_lock_wait_ns = per_core_group_lock_wait_ns.iter().sum();
        let per_group_group_lock_wait_ns = self
            .inner
            .per_group_group_lock_wait_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_group_engine_exec_ns = self
            .inner
            .per_core_group_engine_exec_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_engine_exec_ns = per_core_group_engine_exec_ns.iter().sum();
        let per_group_group_engine_exec_ns = self
            .inner
            .per_group_group_engine_exec_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_group_group_mailbox_depth = self
            .inner
            .per_group_group_mailbox_depth
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_mailbox_depth = per_group_group_mailbox_depth.iter().sum();
        let per_group_group_mailbox_max_depth = self
            .inner
            .per_group_group_mailbox_max_depth
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_mailbox_max_depth = per_group_group_mailbox_max_depth
            .iter()
            .copied()
            .max()
            .unwrap_or(0);
        let per_group_group_mailbox_full_events = self
            .inner
            .per_group_group_mailbox_full_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_mailbox_full_events = per_group_group_mailbox_full_events.iter().sum();
        let per_core_raft_write_many_batches = self
            .inner
            .per_core_raft_write_many_batches
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_batches = per_core_raft_write_many_batches.iter().sum();
        let per_group_raft_write_many_batches = self
            .inner
            .per_group_raft_write_many_batches
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_commands = self
            .inner
            .per_core_raft_write_many_commands
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_commands = per_core_raft_write_many_commands.iter().sum();
        let per_group_raft_write_many_commands = self
            .inner
            .per_group_raft_write_many_commands
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_logical_commands = self
            .inner
            .per_core_raft_write_many_logical_commands
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_logical_commands =
            per_core_raft_write_many_logical_commands.iter().sum();
        let per_group_raft_write_many_logical_commands = self
            .inner
            .per_group_raft_write_many_logical_commands
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_responses = self
            .inner
            .per_core_raft_write_many_responses
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_responses = per_core_raft_write_many_responses.iter().sum();
        let per_group_raft_write_many_responses = self
            .inner
            .per_group_raft_write_many_responses
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_submit_ns = self
            .inner
            .per_core_raft_write_many_submit_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_submit_ns = per_core_raft_write_many_submit_ns.iter().sum();
        let per_group_raft_write_many_submit_ns = self
            .inner
            .per_group_raft_write_many_submit_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_response_ns = self
            .inner
            .per_core_raft_write_many_response_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_response_ns = per_core_raft_write_many_response_ns.iter().sum();
        let per_group_raft_write_many_response_ns = self
            .inner
            .per_group_raft_write_many_response_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_apply_entries = self
            .inner
            .per_core_raft_apply_entries
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_apply_entries = per_core_raft_apply_entries.iter().sum();
        let per_group_raft_apply_entries = self
            .inner
            .per_group_raft_apply_entries
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_apply_ns = self
            .inner
            .per_core_raft_apply_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_apply_ns = per_core_raft_apply_ns.iter().sum();
        let per_group_raft_apply_ns = self
            .inner
            .per_group_raft_apply_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_live_read_waiters = self
            .inner
            .per_core_live_read_waiters
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let live_read_waiters = per_core_live_read_waiters.iter().sum();
        let per_core_live_read_backpressure_events = self
            .inner
            .per_core_live_read_backpressure_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let live_read_backpressure_events = per_core_live_read_backpressure_events.iter().sum();
        let per_core_routed_requests = self
            .inner
            .per_core_routed_requests
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let routed_requests = per_core_routed_requests.iter().sum();
        let per_core_mailbox_send_wait_ns = self
            .inner
            .per_core_mailbox_send_wait_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let mailbox_send_wait_ns = per_core_mailbox_send_wait_ns.iter().sum();
        let per_core_mailbox_full_events = self
            .inner
            .per_core_mailbox_full_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let mailbox_full_events = per_core_mailbox_full_events.iter().sum();
        let per_core_wal_batches = self
            .inner
            .per_core_wal_batches
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let wal_batches = per_core_wal_batches.iter().sum();
        let per_group_wal_batches = self
            .inner
            .per_group_wal_batches
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_wal_records = self
            .inner
            .per_core_wal_records
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let wal_records = per_core_wal_records.iter().sum();
        let per_group_wal_records = self
            .inner
            .per_group_wal_records
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_wal_write_ns = self
            .inner
            .per_core_wal_write_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let wal_write_ns = per_core_wal_write_ns.iter().sum();
        let per_group_wal_write_ns = self
            .inner
            .per_group_wal_write_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_wal_sync_ns = self
            .inner
            .per_core_wal_sync_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let wal_sync_ns = per_core_wal_sync_ns.iter().sum();
        let per_group_wal_sync_ns = self
            .inner
            .per_group_wal_sync_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let cold_flush_uploads = self.inner.cold_flush_uploads.load_relaxed();
        let cold_flush_upload_bytes = self.inner.cold_flush_upload_bytes.load_relaxed();
        let cold_flush_upload_ns = self.inner.cold_flush_upload_ns.load_relaxed();
        let cold_flush_publishes = self.inner.cold_flush_publishes.load_relaxed();
        let cold_flush_publish_bytes = self.inner.cold_flush_publish_bytes.load_relaxed();
        let cold_flush_publish_ns = self.inner.cold_flush_publish_ns.load_relaxed();
        let cold_orphan_cleanup_attempts = self.inner.cold_orphan_cleanup_attempts.load_relaxed();
        let cold_orphan_cleanup_errors = self.inner.cold_orphan_cleanup_errors.load_relaxed();
        let cold_orphan_bytes = self.inner.cold_orphan_bytes.load_relaxed();
        let cold_gc_reclaimed = self.inner.cold_gc_reclaimed.load_relaxed();
        let cold_gc_errors = self.inner.cold_gc_errors.load_relaxed();
        let cold_flush_write_errors = self.inner.cold_flush_write_errors.load_relaxed();
        let per_group_cold_hot_bytes = self
            .inner
            .per_group_cold_hot_bytes
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let cold_hot_bytes = per_group_cold_hot_bytes.iter().sum();
        let per_group_cold_hot_bytes_max = self
            .inner
            .per_group_cold_hot_bytes_max
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let cold_hot_group_bytes_max = per_group_cold_hot_bytes_max
            .iter()
            .copied()
            .max()
            .unwrap_or(0);
        let cold_hot_stream_bytes_max = self.inner.cold_hot_stream_bytes_max.load_relaxed();
        let per_core_cold_backpressure_events = self
            .inner
            .per_core_cold_backpressure_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let cold_backpressure_events = per_core_cold_backpressure_events.iter().sum();
        let per_group_cold_backpressure_events = self
            .inner
            .per_group_cold_backpressure_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let cold_backpressure_bytes = self.inner.cold_backpressure_bytes.load_relaxed();

        RuntimeMetricsSnapshot {
            accepted_appends,
            per_core_appends,
            per_group_appends,
            applied_mutations,
            per_core_applied_mutations,
            per_group_applied_mutations,
            mutation_apply_ns,
            per_core_mutation_apply_ns,
            per_group_mutation_apply_ns,
            group_lock_wait_ns,
            per_core_group_lock_wait_ns,
            per_group_group_lock_wait_ns,
            group_engine_exec_ns,
            per_core_group_engine_exec_ns,
            per_group_group_engine_exec_ns,
            group_mailbox_depth,
            per_group_group_mailbox_depth,
            group_mailbox_max_depth,
            per_group_group_mailbox_max_depth,
            group_mailbox_full_events,
            per_group_group_mailbox_full_events,
            raft_write_many_batches,
            per_core_raft_write_many_batches,
            per_group_raft_write_many_batches,
            raft_write_many_commands,
            per_core_raft_write_many_commands,
            per_group_raft_write_many_commands,
            raft_write_many_logical_commands,
            per_core_raft_write_many_logical_commands,
            per_group_raft_write_many_logical_commands,
            raft_write_many_responses,
            per_core_raft_write_many_responses,
            per_group_raft_write_many_responses,
            raft_write_many_submit_ns,
            per_core_raft_write_many_submit_ns,
            per_group_raft_write_many_submit_ns,
            raft_write_many_response_ns,
            per_core_raft_write_many_response_ns,
            per_group_raft_write_many_response_ns,
            raft_apply_entries,
            per_core_raft_apply_entries,
            per_group_raft_apply_entries,
            raft_apply_ns,
            per_core_raft_apply_ns,
            per_group_raft_apply_ns,
            live_read_waiters,
            per_core_live_read_waiters,
            live_read_backpressure_events,
            per_core_live_read_backpressure_events,
            routed_requests,
            per_core_routed_requests,
            mailbox_send_wait_ns,
            per_core_mailbox_send_wait_ns,
            mailbox_full_events,
            per_core_mailbox_full_events,
            wal_batches,
            per_core_wal_batches,
            per_group_wal_batches,
            wal_records,
            per_core_wal_records,
            per_group_wal_records,
            wal_write_ns,
            per_core_wal_write_ns,
            per_group_wal_write_ns,
            wal_sync_ns,
            per_core_wal_sync_ns,
            per_group_wal_sync_ns,
            cold_flush_uploads,
            cold_flush_upload_bytes,
            cold_flush_upload_ns,
            cold_flush_publishes,
            cold_flush_publish_bytes,
            cold_flush_publish_ns,
            cold_orphan_cleanup_attempts,
            cold_orphan_cleanup_errors,
            cold_gc_reclaimed,
            cold_gc_errors,
            cold_flush_write_errors,
            cold_orphan_bytes,
            cold_hot_bytes,
            per_group_cold_hot_bytes,
            cold_hot_group_bytes_max,
            per_group_cold_hot_bytes_max,
            cold_hot_stream_bytes_max,
            cold_backpressure_events,
            per_core_cold_backpressure_events,
            per_group_cold_backpressure_events,
            cold_backpressure_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMetricsSnapshot {
    pub accepted_appends: u64,
    pub per_core_appends: Vec<u64>,
    pub per_group_appends: Vec<u64>,
    pub applied_mutations: u64,
    pub per_core_applied_mutations: Vec<u64>,
    pub per_group_applied_mutations: Vec<u64>,
    pub mutation_apply_ns: u64,
    pub per_core_mutation_apply_ns: Vec<u64>,
    pub per_group_mutation_apply_ns: Vec<u64>,
    pub group_lock_wait_ns: u64,
    pub per_core_group_lock_wait_ns: Vec<u64>,
    pub per_group_group_lock_wait_ns: Vec<u64>,
    pub group_engine_exec_ns: u64,
    pub per_core_group_engine_exec_ns: Vec<u64>,
    pub per_group_group_engine_exec_ns: Vec<u64>,
    pub group_mailbox_depth: u64,
    pub per_group_group_mailbox_depth: Vec<u64>,
    pub group_mailbox_max_depth: u64,
    pub per_group_group_mailbox_max_depth: Vec<u64>,
    pub group_mailbox_full_events: u64,
    pub per_group_group_mailbox_full_events: Vec<u64>,
    pub raft_write_many_batches: u64,
    pub per_core_raft_write_many_batches: Vec<u64>,
    pub per_group_raft_write_many_batches: Vec<u64>,
    pub raft_write_many_commands: u64,
    pub per_core_raft_write_many_commands: Vec<u64>,
    pub per_group_raft_write_many_commands: Vec<u64>,
    pub raft_write_many_logical_commands: u64,
    pub per_core_raft_write_many_logical_commands: Vec<u64>,
    pub per_group_raft_write_many_logical_commands: Vec<u64>,
    pub raft_write_many_responses: u64,
    pub per_core_raft_write_many_responses: Vec<u64>,
    pub per_group_raft_write_many_responses: Vec<u64>,
    pub raft_write_many_submit_ns: u64,
    pub per_core_raft_write_many_submit_ns: Vec<u64>,
    pub per_group_raft_write_many_submit_ns: Vec<u64>,
    pub raft_write_many_response_ns: u64,
    pub per_core_raft_write_many_response_ns: Vec<u64>,
    pub per_group_raft_write_many_response_ns: Vec<u64>,
    pub raft_apply_entries: u64,
    pub per_core_raft_apply_entries: Vec<u64>,
    pub per_group_raft_apply_entries: Vec<u64>,
    pub raft_apply_ns: u64,
    pub per_core_raft_apply_ns: Vec<u64>,
    pub per_group_raft_apply_ns: Vec<u64>,
    pub live_read_waiters: u64,
    pub per_core_live_read_waiters: Vec<u64>,
    pub live_read_backpressure_events: u64,
    pub per_core_live_read_backpressure_events: Vec<u64>,
    pub routed_requests: u64,
    pub per_core_routed_requests: Vec<u64>,
    pub mailbox_send_wait_ns: u64,
    pub per_core_mailbox_send_wait_ns: Vec<u64>,
    pub mailbox_full_events: u64,
    pub per_core_mailbox_full_events: Vec<u64>,
    pub wal_batches: u64,
    pub per_core_wal_batches: Vec<u64>,
    pub per_group_wal_batches: Vec<u64>,
    pub wal_records: u64,
    pub per_core_wal_records: Vec<u64>,
    pub per_group_wal_records: Vec<u64>,
    pub wal_write_ns: u64,
    pub per_core_wal_write_ns: Vec<u64>,
    pub per_group_wal_write_ns: Vec<u64>,
    pub wal_sync_ns: u64,
    pub per_core_wal_sync_ns: Vec<u64>,
    pub per_group_wal_sync_ns: Vec<u64>,
    pub cold_flush_uploads: u64,
    pub cold_flush_upload_bytes: u64,
    pub cold_flush_upload_ns: u64,
    pub cold_flush_publishes: u64,
    pub cold_flush_publish_bytes: u64,
    pub cold_flush_publish_ns: u64,
    pub cold_orphan_cleanup_attempts: u64,
    pub cold_orphan_cleanup_errors: u64,
    pub cold_orphan_bytes: u64,
    pub cold_gc_reclaimed: u64,
    pub cold_gc_errors: u64,
    pub cold_flush_write_errors: u64,
    pub cold_hot_bytes: u64,
    pub per_group_cold_hot_bytes: Vec<u64>,
    pub cold_hot_group_bytes_max: u64,
    pub per_group_cold_hot_bytes_max: Vec<u64>,
    pub cold_hot_stream_bytes_max: u64,
    pub cold_backpressure_events: u64,
    pub per_core_cold_backpressure_events: Vec<u64>,
    pub per_group_cold_backpressure_events: Vec<u64>,
    pub cold_backpressure_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMailboxSnapshot {
    pub depths: Vec<usize>,
    pub capacities: Vec<usize>,
}

#[derive(Debug)]
pub(crate) struct RuntimeMetricsInner {
    pub(crate) per_core_appends: Vec<PaddedAtomicU64>,
    pub(crate) per_group_appends: Vec<PaddedAtomicU64>,
    pub(crate) per_core_applied_mutations: Vec<PaddedAtomicU64>,
    pub(crate) per_group_applied_mutations: Vec<PaddedAtomicU64>,
    pub(crate) per_core_mutation_apply_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_group_mutation_apply_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_core_group_lock_wait_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_group_group_lock_wait_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_core_group_engine_exec_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_group_group_engine_exec_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_group_group_mailbox_depth: Vec<PaddedAtomicU64>,
    pub(crate) per_group_group_mailbox_max_depth: Vec<PaddedAtomicU64>,
    pub(crate) per_group_group_mailbox_full_events: Vec<PaddedAtomicU64>,
    pub(crate) per_core_raft_write_many_batches: Vec<PaddedAtomicU64>,
    pub(crate) per_group_raft_write_many_batches: Vec<PaddedAtomicU64>,
    pub(crate) per_core_raft_write_many_commands: Vec<PaddedAtomicU64>,
    pub(crate) per_group_raft_write_many_commands: Vec<PaddedAtomicU64>,
    pub(crate) per_core_raft_write_many_logical_commands: Vec<PaddedAtomicU64>,
    pub(crate) per_group_raft_write_many_logical_commands: Vec<PaddedAtomicU64>,
    pub(crate) per_core_raft_write_many_responses: Vec<PaddedAtomicU64>,
    pub(crate) per_group_raft_write_many_responses: Vec<PaddedAtomicU64>,
    pub(crate) per_core_raft_write_many_submit_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_group_raft_write_many_submit_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_core_raft_write_many_response_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_group_raft_write_many_response_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_core_raft_apply_entries: Vec<PaddedAtomicU64>,
    pub(crate) per_group_raft_apply_entries: Vec<PaddedAtomicU64>,
    pub(crate) per_core_raft_apply_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_group_raft_apply_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_core_live_read_waiters: Vec<PaddedAtomicU64>,
    pub(crate) per_core_live_read_backpressure_events: Vec<PaddedAtomicU64>,
    pub(crate) per_core_routed_requests: Vec<PaddedAtomicU64>,
    pub(crate) per_core_mailbox_send_wait_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_core_mailbox_full_events: Vec<PaddedAtomicU64>,
    pub(crate) per_core_wal_batches: Vec<PaddedAtomicU64>,
    pub(crate) per_group_wal_batches: Vec<PaddedAtomicU64>,
    pub(crate) per_core_wal_records: Vec<PaddedAtomicU64>,
    pub(crate) per_group_wal_records: Vec<PaddedAtomicU64>,
    pub(crate) per_core_wal_write_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_group_wal_write_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_core_wal_sync_ns: Vec<PaddedAtomicU64>,
    pub(crate) per_group_wal_sync_ns: Vec<PaddedAtomicU64>,
    pub(crate) cold_flush_uploads: PaddedAtomicU64,
    pub(crate) cold_flush_upload_bytes: PaddedAtomicU64,
    pub(crate) cold_flush_upload_ns: PaddedAtomicU64,
    pub(crate) cold_flush_publishes: PaddedAtomicU64,
    pub(crate) cold_flush_publish_bytes: PaddedAtomicU64,
    pub(crate) cold_flush_publish_ns: PaddedAtomicU64,
    pub(crate) cold_orphan_cleanup_attempts: PaddedAtomicU64,
    pub(crate) cold_orphan_cleanup_errors: PaddedAtomicU64,
    pub(crate) cold_gc_reclaimed: PaddedAtomicU64,
    pub(crate) cold_gc_errors: PaddedAtomicU64,
    pub(crate) cold_flush_write_errors: PaddedAtomicU64,
    pub(crate) cold_orphan_bytes: PaddedAtomicU64,
    pub(crate) per_group_cold_hot_bytes: Vec<PaddedAtomicU64>,
    pub(crate) per_group_cold_hot_bytes_max: Vec<PaddedAtomicU64>,
    pub(crate) cold_hot_stream_bytes_max: PaddedAtomicU64,
    pub(crate) per_core_cold_backpressure_events: Vec<PaddedAtomicU64>,
    pub(crate) per_group_cold_backpressure_events: Vec<PaddedAtomicU64>,
    pub(crate) cold_backpressure_bytes: PaddedAtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RaftWriteManySample {
    pub(crate) command_count: u64,
    pub(crate) logical_command_count: u64,
    pub(crate) response_count: u64,
    pub(crate) submit_ns: u64,
    pub(crate) response_ns: u64,
}

impl RuntimeMetricsInner {
    pub(crate) fn new(core_count: usize, raft_group_count: usize) -> Self {
        Self {
            per_core_appends: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_appends: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_applied_mutations: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_applied_mutations: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_mutation_apply_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_mutation_apply_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_group_lock_wait_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_group_lock_wait_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_group_engine_exec_ns: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_group_engine_exec_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_group_mailbox_depth: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_group_mailbox_max_depth: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_group_mailbox_full_events: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_batches: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_batches: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_commands: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_commands: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_logical_commands: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_logical_commands: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_responses: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_responses: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_submit_ns: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_submit_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_response_ns: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_response_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_apply_entries: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_raft_apply_entries: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_apply_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_raft_apply_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_live_read_waiters: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_core_live_read_backpressure_events: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_routed_requests: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_core_mailbox_send_wait_ns: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_mailbox_full_events: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_wal_batches: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_wal_batches: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_wal_records: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_wal_records: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_wal_write_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_wal_write_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_wal_sync_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_wal_sync_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            cold_flush_uploads: PaddedAtomicU64::new(0),
            cold_flush_upload_bytes: PaddedAtomicU64::new(0),
            cold_flush_upload_ns: PaddedAtomicU64::new(0),
            cold_flush_publishes: PaddedAtomicU64::new(0),
            cold_flush_publish_bytes: PaddedAtomicU64::new(0),
            cold_flush_publish_ns: PaddedAtomicU64::new(0),
            cold_orphan_cleanup_attempts: PaddedAtomicU64::new(0),
            cold_orphan_cleanup_errors: PaddedAtomicU64::new(0),
            cold_gc_reclaimed: PaddedAtomicU64::new(0),
            cold_gc_errors: PaddedAtomicU64::new(0),
            cold_flush_write_errors: PaddedAtomicU64::new(0),
            cold_orphan_bytes: PaddedAtomicU64::new(0),
            per_group_cold_hot_bytes: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_cold_hot_bytes_max: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            cold_hot_stream_bytes_max: PaddedAtomicU64::new(0),
            per_core_cold_backpressure_events: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_cold_backpressure_events: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            cold_backpressure_bytes: PaddedAtomicU64::new(0),
        }
    }

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
        let depth = self.per_group_group_mailbox_depth[group_index].fetch_add_relaxed(1) + 1;
        self.per_group_group_mailbox_max_depth[group_index].fetch_max_relaxed(depth);
    }

    pub(crate) fn record_group_mailbox_dequeued(&self, group_id: RaftGroupId) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_group_group_mailbox_depth[group_index].fetch_sub_relaxed(1);
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

    pub(crate) fn record_cold_orphan_cleanup(&self, bytes: u64, cleanup_failed: bool) {
        self.cold_orphan_cleanup_attempts.fetch_add_relaxed(1);
        if cleanup_failed {
            self.cold_orphan_cleanup_errors.fetch_add_relaxed(1);
            self.cold_orphan_bytes.fetch_add_relaxed(bytes);
        }
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
    if !err.message().contains("ColdBackpressure") {
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
    let RuntimeError::GroupEngine { message, .. } = err else {
        return false;
    };
    message.contains("StreamGone")
        || message.contains("StreamNotFound")
        || (message.contains("InvalidColdFlush")
            && (message.contains("beyond stream")
                || message.contains("does not match the start of a hot payload segment")
                || message.contains("must start at the hot prefix")
                || message.contains("does not cover contiguous hot payload")
                || message.contains("does not cover contiguous hot payload segments")
                || message.contains("exceeds stream")
                || message.contains("non-contiguous hot payload metadata")))
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

    pub(crate) fn fetch_max_relaxed(&self, value: u64) {
        self.value.fetch_max(value, Ordering::Relaxed);
    }

    pub(crate) fn store_relaxed(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
    }
}
