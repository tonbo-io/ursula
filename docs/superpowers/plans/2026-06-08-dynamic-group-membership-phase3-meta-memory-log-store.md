# Dynamic Group Membership Phase 3 Meta Memory Log Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an OpenRaft in-memory log store for `MetaRaftTypeConfig` so the meta group can be bootstrapped in a later phase without touching durable file journal format yet.

**Architecture:** Generalize the existing data-group in-memory log store over `RaftTypeConfig`, then keep `RaftGroupLogStore` as the data-group alias and add `MetaRaftLogStore` as the meta-group alias. File-backed log storage remains protobuf/data-group-only in this phase; durable meta log storage is deferred to a dedicated phase.

**Tech Stack:** Rust 2024, OpenRaft 0.10 `RaftLogStorage`/`RaftLogReader`, `Arc<Mutex<_>>`, `MetaRaftTypeConfig`, focused async unit tests.

---

## Scope Check

This phase only creates reusable in-memory OpenRaft log storage for the meta group. It does not wire bootstrap, networking, admin APIs, CLI, data-group membership migration, or durable meta file storage.

Context7 OpenRaft docs confirm the relevant storage contract:

- `RaftLogReader::try_get_log_entries` returns cloned entries in a requested range.
- `RaftLogReader::read_vote` returns the stored vote.
- `RaftLogStorage::get_log_state` returns `last_purged_log_id` and latest log id.
- `RaftLogStorage::append` must call `IOFlushed::io_completed` after entries are stored.
- `truncate_after`, `purge`, `save_vote`, `save_committed`, and `read_committed` are the storage surface this phase must preserve.

## File Structure

- Modify `crates/ursula-raft/src/log_store/mod.rs`: make the in-memory log state generic over `RaftTypeConfig`, and make log consistency helpers generic.
- Modify `crates/ursula-raft/src/log_store/memory.rs`: replace the data-group-only store struct with `MemoryRaftLogStore<C>`, keep `RaftGroupLogStore` as an alias, and add `MetaRaftLogStore`.
- Modify `crates/ursula-raft/src/lib.rs`: export `MetaRaftLogStore`.
- Modify `crates/ursula-raft/src/tests.rs`: add meta log-store tests while preserving existing data-group log-store tests.

---

### Task 1: Add Failing Meta Log-Store Tests

**Files:**
- Modify: `crates/ursula-raft/src/tests.rs`

- [x] **Step 1: Add imports**

Add imports for meta commands and OpenRaft entry construction near the existing test imports if missing:

```rust
use ursula_control::ControlCommand;
use ursula_shard::RaftGroupId;
```

- [x] **Step 2: Add meta test helpers**

Add helpers near the existing `log_id`/`normal_entry` helpers:

```rust
type MetaLeaderId = <MetaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

fn meta_log_id(index: u64) -> LogId<MetaLeaderId> {
    LogId {
        leader_id: MetaLeaderId::new(1, 1),
        index,
    }
}

fn meta_entry(
    index: u64,
    command: ControlCommand,
) -> <MetaRaftTypeConfig as openraft::RaftTypeConfig>::Entry {
    <MetaRaftTypeConfig as openraft::RaftTypeConfig>::Entry::new(
        meta_log_id(index),
        EntryPayload::Normal(command),
    )
}

fn register_node_command(node_id: u64) -> ControlCommand {
    ControlCommand::RegisterNode {
        node_id,
        client_url: format!("http://node{node_id}:4491"),
        cluster_url: format!("http://node{node_id}:4492"),
        labels: BTreeMap::new(),
        now_ms: 10,
    }
}
```

- [x] **Step 3: Add append/read/truncate/purge test**

Add this async test:

```rust
#[tokio::test]
async fn meta_raft_log_store_appends_reads_truncates_and_purges() {
    let mut store = MetaRaftLogStore::shared();
    store
        .append(
            vec![
                meta_entry(1, register_node_command(1)),
                meta_entry(2, register_node_command(2)),
                meta_entry(3, register_node_command(3)),
            ],
            IOFlushed::noop(),
        )
        .await
        .expect("append meta log entries");

    let state = store.get_log_state().await.expect("log state");
    assert_eq!(state.last_purged_log_id, None);
    assert_eq!(state.last_log_id, Some(meta_log_id(3)));

    let mut reader = store.get_log_reader().await;
    let entries = reader
        .try_get_log_entries(1..4)
        .await
        .expect("read meta entries");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].log_id, meta_log_id(1));
    assert_eq!(entries[2].log_id, meta_log_id(3));

    store
        .truncate_after(Some(meta_log_id(1)))
        .await
        .expect("truncate meta log");
    assert_eq!(
        store.get_log_state().await.expect("log state").last_log_id,
        Some(meta_log_id(1))
    );

    store
        .append(
            vec![
                meta_entry(2, register_node_command(4)),
                meta_entry(3, register_node_command(5)),
            ],
            IOFlushed::noop(),
        )
        .await
        .expect("append after truncate");
    store.purge(meta_log_id(2)).await.expect("purge meta log");

    let state = store.get_log_state().await.expect("log state after purge");
    assert_eq!(state.last_purged_log_id, Some(meta_log_id(2)));
    assert_eq!(state.last_log_id, Some(meta_log_id(3)));

    let entries = reader
        .try_get_log_entries(1..4)
        .await
        .expect("read after purge");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].log_id, meta_log_id(3));
}
```

- [x] **Step 4: Add vote/committed test**

Add this async test:

```rust
#[tokio::test]
async fn meta_raft_log_store_persists_vote_and_committed_pointer() {
    let mut store = MetaRaftLogStore::shared();
    let vote: VoteOf<MetaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

    store.save_vote(&vote).await.expect("save vote");
    let mut reader = store.get_log_reader().await;
    assert_eq!(reader.read_vote().await.expect("read vote"), Some(vote));

    store
        .save_committed(Some(meta_log_id(9)))
        .await
        .expect("save committed");
    assert_eq!(
        store.read_committed().await.expect("read committed"),
        Some(meta_log_id(9))
    );
}
```

- [x] **Step 5: Add hole rejection test**

Add this async test:

```rust
#[tokio::test]
async fn meta_raft_log_store_rejects_holes() {
    let mut store = MetaRaftLogStore::shared();
    let err = store
        .append(
            vec![
                meta_entry(1, register_node_command(1)),
                meta_entry(3, register_node_command(3)),
            ],
            IOFlushed::noop(),
        )
        .await
        .expect_err("hole should be rejected");

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    store
        .append(
            vec![meta_entry(1, register_node_command(1))],
            IOFlushed::noop(),
        )
        .await
        .expect("append first entry");
    let err = store
        .append(
            vec![meta_entry(3, register_node_command(3))],
            IOFlushed::noop(),
        )
        .await
        .expect_err("cross-append hole should be rejected");

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}
```

- [x] **Step 6: Run the failing test**

Run:

```bash
cargo test -p ursula-raft meta_raft_log_store_appends_reads_truncates_and_purges
```

Expected: FAIL because `MetaRaftLogStore` is not defined/exported.

---

### Task 2: Generalize the In-Memory Log Store

**Files:**
- Modify: `crates/ursula-raft/src/log_store/mod.rs`
- Modify: `crates/ursula-raft/src/log_store/memory.rs`

- [x] **Step 1: Make the inner state generic**

In `crates/ursula-raft/src/log_store/mod.rs`, replace the concrete `RaftGroupLogStoreInner` struct with:

```rust
#[derive(Debug, Clone, Default)]
pub(crate) struct MemoryRaftLogStoreInner<C>
where
    C: openraft::RaftTypeConfig,
    C::Entry: Clone,
{
    last_purged_log_id: Option<LogIdOf<C>>,
    committed: Option<LogIdOf<C>>,
    entries: BTreeMap<u64, EntryOf<C>>,
    vote: Option<VoteOf<C>>,
}

pub(crate) type RaftGroupLogStoreInner = MemoryRaftLogStoreInner<UrsulaRaftTypeConfig>;
```

- [x] **Step 2: Make consistency helpers generic**

Update helpers to use `C: openraft::RaftTypeConfig`:

```rust
pub(crate) fn ensure_consecutive_entries<C>(entries: &[EntryOf<C>]) -> Result<(), io::Error>
where
    C: openraft::RaftTypeConfig,
    C::Entry: Clone,
{
    for pair in entries.windows(2) {
        let current = pair[0].log_id.index;
        let next = pair[1].log_id.index;
        if next != current + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log entries are not consecutive: {current} then {next}"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn ensure_log_append_boundary<C>(
    inner: &MemoryRaftLogStoreInner<C>,
    entries: &[EntryOf<C>],
) -> Result<(), io::Error>
where
    C: openraft::RaftTypeConfig,
    C::Entry: Clone,
{
    let Some(first_entry) = entries.first() else {
        return Ok(());
    };
    let Some(last_existing_index) = inner.entries.keys().next_back().copied() else {
        return Ok(());
    };

    let first_append_index = first_entry.log_id.index;
    if first_append_index > last_existing_index + 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raft log store has a hole: {last_existing_index} then {first_append_index}"),
        ));
    }

    Ok(())
}

pub(crate) fn ensure_consecutive_log<C>(
    entries: &BTreeMap<u64, EntryOf<C>>,
) -> Result<(), io::Error>
where
    C: openraft::RaftTypeConfig,
    C::Entry: Clone,
{
    let mut previous = None;
    for index in entries.keys().copied() {
        if let Some(previous) = previous
            && index != previous + 1
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log store has a hole: {previous} then {index}"),
            ));
        }
        previous = Some(index);
    }
    Ok(())
}
```

- [x] **Step 3: Replace concrete memory store with generic store**

In `crates/ursula-raft/src/log_store/memory.rs`, introduce:

```rust
#[derive(Debug, Default)]
pub struct MemoryRaftLogStore<C>
where
    C: openraft::RaftTypeConfig,
    C::Entry: Clone,
{
    inner: Mutex<MemoryRaftLogStoreInner<C>>,
}

pub type RaftGroupLogStore = MemoryRaftLogStore<UrsulaRaftTypeConfig>;
pub type MetaRaftLogStore = MemoryRaftLogStore<crate::MetaRaftTypeConfig>;
```

Keep `new`, `shared`, and `lock_inner` on `MemoryRaftLogStore<C>`, and implement `RaftLogReader<C>` / `RaftLogStorage<C>` for `Arc<MemoryRaftLogStore<C>>`.

- [x] **Step 4: Run the first meta log-store test**

Run:

```bash
cargo test -p ursula-raft meta_raft_log_store_appends_reads_truncates_and_purges
```

Expected: PASS.

---

### Task 3: Export Public Meta Store API

**Files:**
- Modify: `crates/ursula-raft/src/log_store/mod.rs`
- Modify: `crates/ursula-raft/src/lib.rs`
- Modify: `crates/ursula-raft/src/tests.rs`

- [x] **Step 1: Export from `log_store`**

In `crates/ursula-raft/src/log_store/mod.rs`, export:

```rust
pub use memory::MemoryRaftLogStore;
pub use memory::MetaRaftLogStore;
pub use memory::RaftGroupLogStore;
```

- [x] **Step 2: Export from crate root**

In `crates/ursula-raft/src/lib.rs`, add:

```rust
pub use log_store::MemoryRaftLogStore;
pub use log_store::MetaRaftLogStore;
```

- [x] **Step 3: Add public export test**

Add this test:

```rust
#[test]
fn public_meta_log_store_type_is_exported_from_crate_root() {
    let _store = crate::MetaRaftLogStore::shared();
    let _generic = crate::MemoryRaftLogStore::<crate::MetaRaftTypeConfig>::shared();
}
```

- [x] **Step 4: Run export test**

Run:

```bash
cargo test -p ursula-raft public_meta_log_store_type_is_exported_from_crate_root
```

Expected: PASS.

---

### Task 4: Regression Verification

**Files:**
- Verify: `crates/ursula-raft/src/log_store/mod.rs`
- Verify: `crates/ursula-raft/src/log_store/memory.rs`
- Verify: `crates/ursula-raft/src/tests.rs`

- [x] **Step 1: Run data-group memory log-store regressions**

Run:

```bash
cargo test -p ursula-raft raft_log_store
```

Expected: existing data-group in-memory/file log-store tests PASS.

- [x] **Step 2: Run meta log-store tests**

Run:

```bash
cargo test -p ursula-raft meta_raft_log_store
```

Expected: PASS.

- [x] **Step 3: Run format/lint**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Expected: PASS.

- [x] **Step 4: Commit Phase 3**

Stage and commit:

```bash
git add crates/ursula-raft/src/log_store/mod.rs crates/ursula-raft/src/log_store/memory.rs crates/ursula-raft/src/lib.rs crates/ursula-raft/src/tests.rs docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase3-meta-memory-log-store.md
git commit -m "feat(ursula-raft): add meta memory log store"
```

Expected: commit succeeds.

---

## Self-Review Notes

- Covered: `MetaRaftTypeConfig` can use the same in-memory log-store behavior as data groups, including append/read/truncate/purge, vote persistence, and committed pointer persistence.
- Deferred: meta durable file journal, meta group bootstrap, network routing, admin API/CLI, and migration executor.
- Risk control: existing `RaftGroupLogStore` public name remains available as a type alias, and data-group tests must pass unchanged.
