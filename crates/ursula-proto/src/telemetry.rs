//! Shared storage telemetry contract, independent of runtime and Raft implementations.

/// What one journal write reports beyond its own latency.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalStorageSample {
    /// `fsync` calls the write issued, on files and directories.
    pub fsyncs: u64,
    /// Records made durable by those `fsync`s.
    pub fsync_records: u64,
    /// The current size of the core's journal, all segments.
    pub physical_bytes: u64,
}

/// What a core journal's writer did besides writing batches: rotating
/// segments and reclaiming old ones. Counters add up; gauges replace.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalJournalSample {
    /// `fsync` calls of rotations and reclaim passes, on files and
    /// directories.
    pub fsyncs: u64,
    pub rotations: u64,
    /// Segments deleted, and their bytes.
    pub reclaims: u64,
    pub reclaimed_bytes: u64,
    pub reclaim_ns: u64,
    /// Reclaim passes that stopped on an error and left the journal correct.
    pub reclaim_failures: u64,
    /// Live entry bytes copied out of old segments.
    pub rewritten_bytes: u64,
    /// Gauge: the journal's size, all segments.
    pub physical_bytes: u64,
    /// Gauge: the journal's segments.
    pub segments: u64,
    /// Gauge: sealed segments kept only for lagging groups.
    pub pinned_segments: u64,
    /// Gauge: groups reported lagging to the snapshot driver.
    pub lagging_groups: u64,
}

/// What a read of a group's log cost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalReadSample {
    /// Entries served from the group's cache.
    pub cache_hits: u64,
    /// Entries read from disk.
    pub cache_misses: u64,
    /// Frames read from disk, and their bytes.
    pub disk_reads: u64,
    pub disk_read_bytes: u64,
}

/// The size of a group's log in memory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalMemorySample {
    /// Bytes of cached entries.
    pub cache_bytes: u64,
    /// Entries the group's index holds.
    pub indexed_entries: u64,
}
