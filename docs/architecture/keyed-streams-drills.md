# Keyed streams M4 drills and S3 / multi-node end-to-end runs

Status: all six drills automated and passing, 2026-10-02. They ran locally at the design's durations (10-minute outages, 16 owners) and run in CI with short knobs on every push to `agent/keyed-streams` and `agent/ks-**`. A nightly workflow runs them at full length. This is not the 7-day soak of M4's exit; the drills are the building blocks that soak would schedule.

Design references: `keyed-streams-pi-durable.md` §3.8 (lifecycle, purge, disaster recovery), §5.5 (continuity, versioned namespaces), §6.3 (rolling-upgrade gate), §10 M4 (drills), §11.7 (fault and takeover).

## 1. What runs where

| What | Where | Command |
|---|---|---|
| e2e suite, single node, filesystem indexer (existing) | CI `typescript-e2e` | `npm run test:e2e` |
| e2e suite on S3, single node, plus the S3 lifecycle checks | CI `typescript-e2e-s3` | `E2E_S3=1 npm run test:e2e` |
| e2e suite on S3 through the gateway of 3 nodes, plus the cluster checks | CI `typescript-e2e-s3` | `E2E_NODES=3 E2E_S3=1 npm run test:e2e` |
| the drills, short knobs (30 s outages, 4 owners) | CI job `Keyed fault drills (short)` (`fault-drills`) | `DRILL_OUTAGE_S=30 DRILL_SETTLE_S=5 DRILL_OWNERS=4 DRILL_COMMIT_DEADLINE_MS=10000 npm run test:drills` |
| the drills, design durations (600 s outages, 16 owners) | nightly `keyed-streams-drills.yml` (`Keyed fault drills (nightly)`), or locally | `npm run test:drills` (defaults), nightly uses `DRILL_OWNERS=16 DRILL_SETTLE_S=30` |

All commands run in `clients/pi-durable-ursula` with `URSULA_BIN` set to the release binary. The rolling-upgrade drill also needs `URSULA_OLD_BIN`, main's binary at `e6d8d70`, which `scripts/ks_build_old_ursula.sh` builds with `git archive` (CI caches it). S3 is MinIO: `URSULA_S3_ENDPOINT` names an external one (CI starts it with `scripts/ks_minio_ci.sh`); otherwise each stack spawns `minio` from `PATH` or `MINIO_BIN`. Each stack creates its own S3 bucket. Each drill appends its measurements to `drill-results/results.jsonl` (or `$DRILL_RESULTS_DIR`), which CI uploads as an artifact.

Knobs: `DRILL_OUTAGE_S` (600), `DRILL_SETTLE_S` (20), `DRILL_RECOVERY_S` (180, the deadline for recovery and catch-up), `DRILL_OWNERS` (8), `DRILL_COMMIT_INTERVAL_MS` (100), `DRILL_PAYLOAD_BYTES` (512), `DRILL_OVERLAY_CAP_BYTES` (256 MiB), `DRILL_COMMIT_DEADLINE_MS` (30000).

## 2. The stack

`clients/pi-durable-ursula/test/stack/` is shared by the e2e global setup and the drills:

- `cluster.ts` starts one node, or three behind `ursula gateway`, plus a keyed-mode `ursula indexer`, and raises the feature level. With S3, the node cold store (`[storage.cold] backend = "s3"`, root `ursula`) and the indexer (`--s3-bucket`, `--keyed-s3-root ursula`) share one bucket. The indexer's namespaces therefore live at `ursula/.keyed/{bucket}/{key}/{c:016x}/v{fmt}/`, under the node cold root, which is where stream-delete GC (U22) and bucket purge (U23) look. Every component reaches S3, and the nodes reach the indexer, through a TCP fault proxy (`proxy.ts`). That proxy is how the drills take S3 or keyed-state down, and how blue/green cuts keyed-state over to another indexer pod, without touching the processes.
- `fleet.ts` runs live bounded owners (`UrsulaStorage`, `stateStore: "bounded"`). Each owner commits one Pi entry per step (nested JSON, a non-ASCII string, an exponent number, a 512 B text) and then reads it back on the Session line. On poison it reopens in `fence` mode, as a host does. It counts `StorageRejected` separately: that is what Pi turns into a faulted task (§3.3). The fleet captures every append body, and `verify()` reads the log back and requires every acknowledged commit to be stored byte-for-byte at its Seq.
- `s3.ts` launches MinIO and holds a 60-line SigV4 client (bucket create, `ListObjectsV2`) that the checks use to prove prefixes empty or present.

## 3. End-to-end runs on S3 and on three nodes

On S3 the whole existing e2e suite passes (Pi conformance 23 × {direct, reopen} × both open modes, open/claim/fencing, takeover, indexer restart), plus `test/e2e/s3.e2e.ts`:

- **Stream-delete GC reaches the indexer.** After a keyed stream is published, its namespace has `CURRENT` and `v1/parts/` objects under `ursula/.keyed/`. `DELETE` the stream and the namespace prefix becomes empty within the GC interval. A neighbour stream's namespace stays.
- **Purge drains and erases both prefixes.** Fill a bucket's `{bucket}/` (cold chunks, after a `flush-cold`) and `.keyed/{bucket}/`, then purge it: the report has `bucket_prefix_absent` and `keyed_prefix_absent`, our own S3 listing of both prefixes is empty, and other buckets keep theirs. This passes on one node and on three; on three, the purge is asked of each node in turn until one reports completion.

With `E2E_NODES=3` the whole suite runs through the gateway, plus `test/e2e/cluster.e2e.ts`:

- **Keyed-state on a follower.** Every node answers the same keyed-state request with 200 and identical rows. A follower serves keyed-state from its own replica (its `HEAD` is a local read), so it does not redirect. If it has not yet applied the requested tail, it answers 400 with its record tail, which the owner retries (§11.7). The leader redirect is on writes: an append with a stale `Stream-Record-Match` answers 412 on the leader and 307 (Location = leader) on each follower, which the test uses to find the leader without writing.
- **An owner talking to a follower node directly** commits through the 307s, raises E through flush-waits served by the follower, and reopens there.
- **Owner takeover across nodes.** Owner A on node 1, a `fence` open on node 2: A's next commit fails with `FencedError`, and B sees A's commits plus its own.

**Defect found and fixed.** The owner's HTTP transport left redirects to `fetch`, which cannot replay a `Uint8Array` body on a 307 (`Cannot perform ArrayBuffer.prototype.slice on a detached ArrayBuffer`). Every commit through a follower node therefore failed as a connection error and poisoned at the deadline. The transport now follows 307/308 itself, up to 10 hops, with a fresh copy of the body on each hop. Regression tests are in `test/http.test.ts`. Through the gateway this was invisible, because the gateway follows 307 internally.

## 4. Drills

Every drill asserts its expected outcome. The tables give the two local runs: CI knobs (30 s outages, 4 owners, 10 s commit deadline), and the design durations (600 s outages, 16 owners, 30 s settle, production 30 s commit deadline). Commits are one per owner per ~100 ms. Both runs were on one 10-core laptop shared with other workloads; the numbers are loopback figures, not production latencies.

### 4.1 Indexer outage (`indexer-outage.drill.ts`)

Owners commit through an S3-backed node, the indexer process is killed (SIGKILL) for the outage, then restarted on the same store.

Expected: every owner keeps committing (flush-waits retry; the overlay grows under the cap), zero poison, zero faulted, E reaches each owner's tail-at-recovery, every acknowledged commit byte-identical.

| | 30 s, 4 owners | 600 s, 16 owners |
|---|---|---|
| commits during the outage | 1,151 | 92,250 |
| poison / faulted | 0 / 0 | 0 / 0 |
| max overlay | 285 KB, 308 records | 5.4 MB, 5,792 records (cap 256 MiB) |
| Session-line remote reads | 0 | 0 |
| E caught up after restart | 0.9 s | 1.4 s |
| verified records, mismatches | 1,592, 0 | 101,713, 0 |

At 10 records/s per owner, 10 minutes add about 6,000 records (5.4 MB) to the overlay, far below the cap: an owner would have to write about 450 KB/s for 10 minutes to reach 256 MiB.

### 4.2 S3 outage (`s3-outage.drill.ts`)

Same stack, and the S3 proxy resets every connection for the outage: the node's cold flushes fail, and the indexer can neither read nor publish.

Expected: no faulted task. Poison and failed opens are allowed, and any poisoned owner must reopen. After S3 returns, every owner commits again and E catches up. The node must report cold-flush write errors (proof that the outage was real), and every acknowledged commit must be byte-identical.

| | 30 s, 4 owners | 600 s, 16 owners |
|---|---|---|
| commits during the outage | 1,160 | 90,223 |
| node cold-flush write errors | 19 | 425 |
| poison / faulted / reopens | 0 / 0 / 0 | 0 / 0 / 0 |
| max overlay records | 310 | 5,673 |
| every owner committing again / E caught up | 0.6 s / 0.8 s | 0.6 s / 2.4 s |
| verified records, mismatches | 1,600, 0 | 99,888, 0 |

The log keeps accepting writes during an S3 outage because unflushed bytes stay hot. At this load the hot set never approached `max_hot_size_per_group` (64 MiB) or the cold-health gate (48 MiB), so no write was refused and nothing poisoned. A heavier or longer outage would hit those 503s; the owner treats them as retries and poisons at the commit deadline, which this drill allows. That is the design's "retries and poison only".

### 4.3 Rolling upgrade from main (`rolling-upgrade.drill.ts`)

3 nodes, disk WAL, S3, gateway, all on main's binary (`e6d8d70`) at level 0. A live writer appends 64-byte binary blobs (including non-UTF-8 bytes). Phase A replaces nodes 3, 2, 1 with the new binary one at a time, waiting until every group is writable after each restart, then replaces the gateway and raises the feature level to 1, all while the writer runs. Phase B starts keyed owners (keyed streams need level 1) and rolls all three nodes again, new to new, under that keyed traffic.

Expected: a keyed create is refused before the raise; the level reads ≥ 1 on every replica after it; zero poison and zero faulted; every acknowledged blob is present in order and byte-identical, and identical on all three nodes; every acknowledged keyed commit is byte-identical when read from each node.

| | CI knobs | 16 owners |
|---|---|---|
| keyed create before the raise | 409 | 409 |
| level after the raise | 1 | 1 |
| node restart to writable, phase A (old → new) | 0.3–0.5 s | 0.3–0.7 s |
| node restart to writable, phase B (new → new) | 0.5–0.6 s | 1.8–2.3 s |
| blobs acknowledged (write retries, duplicates) | 1,129 (3, 0) | 4,897 (4, 0) |
| keyed commits during the phase B roll | 563 | 8,267 |
| poison / faulted | 0 / 0 | 0 / 0 |
| byte mismatches (blobs and keyed, every node) | 0 | 0 |

The upgrade from main has no live keyed traffic, by construction: main does not serve `keyed-batch-v1`, and keyed creates are refused until every node runs the new binary and the level is raised (§6.3). The keyed half of the exit criterion is therefore the phase B rolling restart.

### 4.4 Purge (`purge.drill.ts`)

Two buckets of live owners on an S3-backed node. The first bucket's streams are flushed cold, so both `{bucket}/` and `.keyed/{bucket}/` hold objects. Then `DELETE /__ursula/purge/{bucket}` runs while both fleets keep committing.

Expected: the purge completes with the indexer drained; both prefixes are empty right after, and still empty after a settle period while the purged owners keep retrying (no resurrection); the purged owners poison (their streams are gone) and never fault; the other bucket sees zero poison, keeps its objects, and its records are byte-identical.

| | CI knobs | 16 owners |
|---|---|---|
| objects in the purged prefixes before | 29 | 126 |
| purge to complete | 70 ms | 368 ms |
| objects after the purge / after the settle | 0 / 0 | 0 / 0 |
| purged owners: poison / faulted | 4 / 0 | 16 / 0 |
| other bucket: poison / faulted / objects | 0 / 0 / 96 | 0 / 0 / 1,603 |
| verified records, mismatches | 615, 0 | 10,084, 0 |

### 4.5 Restore plus continuity rebuild (`restore.drill.ts`)

Live owners on an S3-backed node. The drill exports every Raft group through the backup API (`GET /__ursula/backup/group/{id}`, the endpoints `ursulactl backup` uses) and lets the owners write until each projection is at least 20 records past the backup. Then the node is destroyed (SIGKILL, memory WAL). A fresh node on the same S3 cold root raises the level and then imports every group (`POST …/import`); an import refuses a backup above the group's level. The indexer stays up throughout, so its namespaces are ahead of the restored log.

Expected: the owners poison at their commit deadline and never fault; the first keyed-state read of every restored stream answers 503 (its namespace is ahead); keyed-state then serves exactly the restored tail (continuity rebuild from record 0); every owner reopens and commits again; every acknowledged commit below the restored tail, and every commit after the restore, is byte-identical.

| | CI knobs | 16 owners |
|---|---|---|
| owners poisoned after the node loss | 10.1 s (deadline 10 s) | 30.0 s (deadline 30 s) |
| first keyed-state answer per stream | 503 × 4 | 503 × 16 |
| rebuilt to the restored tail (all streams) | 0.8 s | 3.3 s |
| poison / faulted / reopens | 4 / 0 / 4 | 16 / 0 / 16 |
| verified records, mismatches | 260, 0 | 4,818, 0 |

Commits acknowledged after the backup are lost: RPO is the export time (§3.8). The drill removes them from the expected set (`FleetOwner.rewind`).

### 4.6 Blue/green projection format rebuild (`blue-green.drill.ts`)

Live owners are served by the "blue" indexer at projection format 1. A "green" pod at format 2 starts on the same object store. The second format comes from the hidden drill knob `--keyed-projection-format`: the same layout, but separate `v2/` namespaces (see §5). While blue keeps serving, the drill builds every stream's `v2/` namespace from record 0 to the stream's tail with the U20 maintenance CLI, `ursula indexer keyed rebuild --projection-format 2` (the incarnation `c` is taken from the blue namespace path). The green pod then starts from those namespaces and catches up to the tail on its first reads. Then the keyed-state proxy is cut over to green under live traffic.

Expected: zero poison and zero faulted; E catches up on green after the cutover; at the same tail both pods answer byte-identical rows for every stream (full paged scans); `v1/` and `v2/` coexist; deleting a stream removes both formats; every acknowledged commit is byte-identical.

| | CI knobs | 16 owners |
|---|---|---|
| green warm-up (all streams) | 45 ms | 95 ms |
| E caught up after the cutover | 1.8 s | 1.8 s |
| poison / faulted | 0 / 0 | 0 / 0 |
| streams with identical blue and green rows | 4 / 4 | 16 / 16 |
| formats side by side; after stream delete | v1, v2; none | v1, v2; none |
| verified records, mismatches | 376, 0 | 5,301, 0 |

### 4.7 Indexer active/standby failover (`indexer-failover.drill.ts`)

One node lists two indexers, each behind its own fault proxy, in `keyed_state.indexer_urls` (primary first); both share the object store. Four bounded owners commit while the primary is SIGKILLed, and a poller reads every owner's keyed state without `min_through_record`. Expected and checked: the node's `keyed_state_upstream` metrics switch `active_pod` to the standby, the standby's request count rises, every owner's E keeps reaching its tail, zero poison or faulted tasks, no served `Stream-Keyed-Through` ever decreases, and every acknowledged commit is stored byte-for-byte. After the primary restarts on its port, the node's `/readyz` prober marks it healthy, `active_pod` returns to 0 and no further read goes to the standby. It does not depend on the outage knobs and takes about ten seconds.

## 5. Choices and gaps

- **Blue/green needs a second format, and U20 is not built.** `ursula-index` gained `KeyedEngineConfig.projection_format` and the hidden indexer flag `--keyed-projection-format`. A namespace now carries its format, and manifests are checked against it. The drill therefore rehearses the procedure (side-by-side build, warm-up, cutover, cleanup) with an identical layout; a real format change will ship its own reader and writer. The warm-up now uses U20's `ursula indexer keyed rebuild --projection-format 2`, which builds a namespace at another format from record 0 up to the source tail and publishes its first `CURRENT` (at the served format it rebuilds in place, blue/green by one CAS). The measured warm-up times above predate that switch, when green was warmed through its internal API. The drill's row-by-row comparison at equal D is the check that `verify` would make.
- **Restore uses the backup endpoints directly,** not `ursulactl`, to avoid building another binary in the drill. It skips the manifest and checksums, not the import path.
- **Keyed-state on followers is served locally,** not redirected (§3). The task asked for a leader redirect. The redirect exists on appends, and the e2e checks both behaviours.
- **The nightly workflow runs only from the default branch** (GitHub schedules). Until this lands on `main`, the full-length run is the local one recorded above.
- **Not covered here:** the 7-day soak with mixed fast and slow harnesses in a hot group; replicated-state and per-harness S3 request budgets (M4 exit); the indexer crash injection of §11.7 (unit-level in `ursula-index`); a real EKS rollout (the upgrade drill runs local processes, not pods).
