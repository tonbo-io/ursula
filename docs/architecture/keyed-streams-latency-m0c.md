# Keyed streams M0c: commit latency, local spike

Status: local spike done 2026-10-02 on one laptop. The production gate (design §10 M0c, 3-AZ and 1-AZ on the target volume) has not been run. This document gives the local numbers, what they can and cannot show, and the exact commands for the multi-AZ run.

Design references: `keyed-streams-pi-durable.md` §9.2 (line model), §10 M0c (exit criteria), §13 Q6 (levers if M0c misses).

## 1. Summary

- **A single machine cannot answer the M0c question.** The exit criteria (mean L ≤ 5 ms, p50 ≤ 4, p99 ≤ 20, p999 ≤ 60) depend on two inter-AZ round trips and on a Linux fsync on the target volume. Neither exists here. Every number below is loopback.
- **Ursula's own share of L, measured, is about 1.7 ms mean and 3 ms p99.** That is a serialized `Stream-Record-Match` JSON append through `ursulagw` to a 3-node cluster running the disk-WAL code path, with near-free flushes (RAM disk), on an otherwise quiet cluster. At 16 concurrent single-writer lines it is 2.4 ms mean, 9 ms p99 and 22 ms p999. Memory WAL gives 0.2 ms at N=1, so the disk-WAL path itself costs about 1.5 ms on loopback.
- **Payload size barely matters.** Commits over 4 KiB (13–50 KiB tool settles) take about 0.4 ms longer than 127 B partials.
- **`Stream-Record-Match` behaved.** 5.1 M matched appends across all retained runs produced zero 412s. 10 transport errors occurred, all in the first run after a cluster start; §6 has the details.
- **The model is in the right range but is not a calibration.** The §9.2 mean-value model reproduces the design table exactly. With the measured local L it predicts 15–22 ms submit→provider for N = 1–16. The real Pi harness, driving real record-match commits against the same cluster, measured 9–12 ms. It is conservative because only 3 commits settle before the provider request in the current Pi (the design assumed 4), and the 1.1 ms Pi overhead per commit looks smaller in practice. At saturation the model errs the other way: with L ≈ 12.7 ms at N=16 the line saturates at 78 commits/s and submit→provider is 820 ms, against 530 ms predicted.
- **Fsync is the dominant production unknown.** On macOS, `sync_data` maps to `F_FULLFSYNC`, which costs 3.9 ms (p99 6 ms) on this SSD. Disk WAL on APFS therefore gives L ≈ 12.4 ms at N=1, with an aggregate ceiling of about 210 commits/s. That is a macOS artifact, not a production prediction, but it shows how fast the line saturates once L passes about 6 ms (the design's 5.5 ms threshold at N=16).
- **Background load hurts tails a lot on one box,** but CPU contention confounds it: the load generator, 3 nodes, the gateway and other workloads shared 10 cores, and the 1-minute load average reached 25. The 50% background cells are an upper bound on interference, not a measurement of it.
- **Side finding: inline snapshots are large.** Without S3, group snapshots fall back to the inline store, whose pointer bytes are serialized as a JSON number array (about 4 bytes of text per byte). This produced 90 MB snapshot files per group, about 240 ms per build, and filled a 6 GiB WAL volume within minutes. The first RAM-disk suite hit WAL disk pressure and was discarded (§4.4). Production uses the S3 snapshot store, so this does not apply there directly, but snapshot build CPU, including the dense record index of JSON streams (design §8 sealing), lands on the thread-per-core loop and shows up in p999.

**Recommendation.** Treat the local floor as about 2 ms of L that is Ursula's own. Run the multi-AZ gate (§7) before deciding on §13 Q6. If production fsync plus two cross-AZ RTTs stay under about 3 ms in the mean, the 5 ms mean budget holds. The p99 ≤ 20 and p999 ≤ 60 targets depend on tail behaviour under real background load and cold flush, and nothing here can predict that.

## 2. What a single machine cannot show

- **No inter-AZ RTT.** Loopback RTT is about 30 µs. Production L includes owner→gateway (same AZ), gateway→leader (possibly cross-AZ, about 0.5–1.5 ms RTT on AWS) and leader→follower replication (cross-AZ RTT plus follower fsync). The M0c setup puts the owner in a non-leader AZ deliberately; the local cluster has no such thing.
- **No production fsync.** Linux `fdatasync` on NVMe or EBS gp3/io2 has a different cost and a different concurrency profile from macOS `F_FULLFSYNC` (3.9 ms on APFS) or a RAM disk (0.02 ms). The two disk configurations here bracket production; they do not represent it.
- **No isolation.** Three voters, the gateway, the bench client, the background generator and, at times, other workloads (load average 4–45) share 10 cores. Production voters have dedicated cores, and the thread-per-core runtime is sensitive to stolen CPU.
- **No S3.** Cold storage is the per-process `memory` backend, and snapshots use the inline store. Cold flush runs (200 ms tick), but its S3 PUT latency and the external snapshot upload path are absent.
- **No 3 runs per AZ topology.** M0c asks for 3-AZ and 1-AZ, and neither exists here.

## 3. Method

### 3.1 Bench mode (`ursula-bench record-match`)

New subcommand in `crates/ursula-bench/src/record_match.rs`:

- One writer task per stream. Each writer keeps exactly one append in flight, and every append carries `Stream-Record-Match: n`, where `n` is the `Stream-Record-Next` returned by its previous append (0 for a fresh stream). This is the Pi Durable owner's commit path: one Session line per harness, one commit in flight.
- Bodies are single JSON objects shaped like a `keyed-batch-v1` record with one put (`{"o":N,"ops":[["p","AWJlbmNoAA",{"t":"…"}]]}`), padded to the target size, so each append is exactly one record. Content type defaults to `application/json`. `--content-type 'application/json; profile=keyed-batch-v1'` works once the profile is wired.
- `--payload-mix pi`: 70% 127 B partials, 15% 200–700 B task/entry commits, 13% 1–4 KiB tool-output deltas and turn settles, and 2% 13–50 KiB tool settles. Mean ≈ 1.15 KB. The PRNG is seeded and deterministic. The mix is heavier than the real Pi text turns measured by the probe (mean 260–460 B), which makes it conservative.
- Reports commits/s, mean/p50/p90/p99/p999/max over measured samples (warm-up excluded), the same per size class, 412 and error counts, and optionally every sample (`--samples-out`).
- `--think-ms` adds idle time between commits (default 0, closed loop).

Design M0c named the flags `--content-type json --record-match` on the existing mode. I added a dedicated subcommand instead, because the existing `multi-stream` mode uses producer headers, octet bodies and a fixed rate, all of which conflict with a matched single-writer line.

### 3.2 Cluster

`scripts/ks_latency_cluster.sh up <disk|memory> <root> <port_base>` starts 3 `ursula server` processes and `ursula gateway` (`ursulagw`) in front, on ports `port_base` (gateway), `port_base+1..3` (nodes) and `port_base+11..13` (admin). Configuration: `runtime.core_count = 2`, `raft.group_count = 32`, `raft.wal.backend = disk|memory`, cold `memory` backend with `flush_interval = 200ms`, membership initialised by node 1, and 8 s settle time after readiness. Optional `KS_SNAPSHOT_LOGS` sets `raft.snapshot_logs_since_last`.

Configurations measured:

| label | WAL | WAL volume | flush cost | snapshots |
|---|---|---|---|---|
| `ramdisk` | disk | 6 GiB HFS+ RAM disk (`hdiutil attach -nomount ram://12582912`) | `F_FULLFSYNC` 0.02 ms | disabled (`snapshot_logs_since_last = 100000000`), see §4.4 |
| `apfs` | disk | internal SSD, APFS | `F_FULLFSYNC` 3.9 ms mean, 6.0 ms p99 (`fsync` alone 0.03 ms) | default (5000); none were built at these rates |
| `memwal` | memory | none | none | default (inline, in memory) |

### 3.3 Matrix

`scripts/ks_latency_suite.sh` starts a **fresh cluster per writer count** (so WAL growth and snapshot state never carry over between cells), runs `scripts/ks_latency_matrix.sh`, saves node-1 metrics, tears down and wipes the data. Per cell: N ∈ {1, 4, 8, 16} writers, each on its own stream; 3 runs; 3 s warm-up plus 20 s measured; Pi payload mix; with and without background load.

Background load = `ursula-bench multi-stream`, 64 streams, 1 KiB octet bodies with producer headers, at about 50% of the configuration's measured closed-loop capacity (64 streams × 1 KiB, 8 s):

| config | capacity | background target | achieved |
|---|---|---|---|
| ramdisk | ≈ 7 900/s | 4 000/s | 3 300–4 030/s |
| memwal | ≈ 18 000/s | 9 000/s | ≈ 9 000/s |
| apfs | ≈ 220/s | 110/s (rounded up to 2/s per stream) | 129/s |

Cold flush stays on (200 ms tick) throughout.

The 1-minute load average is recorded at the start of each run (§8).

### 3.4 Line model and simulation

`scripts/ks_latency_line_model.py`:

- `table` reproduces the design §9.2 table from the model as stated (exact MVA with N closed partial sources, 100 ms re-arm, `S = L̄ + 1.1 ms`, blocking wait `S·(1+Q)`, 4 blocking commits + 3 ms). It prints 24/28/35/62, 29/34/44/93, 33/40/56/133 and 42/55/84/237 ms, identical to the design.
- `mva <L̄>` applies the model to a measured mean.
- `sim <samples>` is a discrete-event simulation of the same FIFO line whose service times are drawn from the measured sample file plus 1.1 ms. It gives a distribution of submit→provider, not just a mean.

### 3.5 Pi probe

`scripts/ks_latency_pi_probe.ts` is the instrumented probe2/probe3 harness with a Storage wrapper whose `commit` first performs a real `Stream-Record-Match` append of the encoded write batch to the cluster (Node `fetch`, keep-alive). It then applies the batch to `MemoryStorage`, which keeps reads local, as in the design's full-resident owner. N−1 conversations stream a long reply (partial sources) and conversation 0 submits 10 short turns back to back. The probe records submit→provider-request, per-commit L as seen by Node, commit sizes, and how many non-partial commits settle between submit and the provider request. The body is `JSON.stringify({o, ops: writes})`, a stand-in for the §4.4 encoder. Sizes match the real write set to within the encoding overhead.

Run it from the probe directory with `node --experimental-strip-types --import ./register.mjs probe4.ts`. The loader maps `@earendil-works/*` onto a local Pi checkout.

### 3.6 Environment

Apple M5, 10 cores, 32 GiB, macOS 26.6.2. Worktree at `566e350` plus this branch, built with `cargo build --release --bin ursula -p ursula` and `cargo build --release -p ursula-bench` on the pinned toolchain. Node v22.22.1. The machine was not idle: the 1-minute load average ranged from 3.3 to 45 across runs, because other build jobs ran concurrently. Load is reported per run. Cells with load above about 15 should be read with care. The `memwal-nosnap` ablation ran at load 30–45 and is reported only for completeness.

## 4. Results: bench

### 4.1 Summary per cell

Latencies are in ms over the pooled samples of all runs in the cell. "run spread" is the range of per-run means. commits/s is the record-match line total (all N writers), averaged over runs.

| config | bg | N | runs | mean ms (run spread) | p50 | p99 | p999 | max | commits/s | bg appends/s | 412 | errors | load avg (1m) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| apfs | no | 1 | 3 | 12.43 (12.20-12.64) | 12.69 | 18.61 | 59.89 | 70.4 | 80 | - | 0 | 0 | 11.56/9.85/7.73 |
| apfs | no | 4 | 3 | 37.07 (36.25-37.91) | 36.30 | 56.10 | 76.54 | 128.3 | 108 | - | 0 | 2 | 3.93/3.34/4.14 |
| apfs | no | 8 | 3 | 52.23 (51.67-53.27) | 51.43 | 88.31 | 129.30 | 139.7 | 153 | - | 0 | 0 | 3.83/4.94/4.15 |
| apfs | no | 16 | 3 | 75.49 (73.23-79.10) | 73.74 | 129.15 | 198.62 | 259.4 | 211 | - | 0 | 8 | 4.23/3.97/3.96 |
| apfs | yes | 1 | 3 | 26.84 (25.57-28.99) | 12.90 | 274.27 | 307.97 | 328.3 | 37 | 129 | 0 | 0 | 6.70/5.06/5.09 |
| apfs | yes | 4 | 3 | 70.00 (69.28-70.88) | 43.81 | 278.01 | 310.03 | 329.2 | 57 | 129 | 0 | 0 | 4.47/4.69/4.23 |
| apfs | yes | 8 | 3 | 139.98 (122.14-167.38) | 76.37 | 422.64 | 497.73 | 631.0 | 57 | 129 | 0 | 0 | 4.54/4.95/4.34 |
| apfs | yes | 16 | 3 | 192.77 (189.53-195.88) | 122.35 | 555.29 | 645.46 | 677.4 | 82 | 129 | 0 | 0 | 4.11/3.92/3.99 |
| memwal | no | 1 | 3 | 0.22 (0.20-0.25) | 0.19 | 0.54 | 1.38 | 70.6 | 4499 | - | 0 | 0 | 12.04/9.91/8.91 |
| memwal | no | 4 | 3 | 0.45 (0.43-0.48) | 0.41 | 0.92 | 5.50 | 78.7 | 8820 | - | 0 | 0 | 10.57/10.78/10.63 |
| memwal | no | 8 | 3 | 0.69 (0.67-0.71) | 0.58 | 1.96 | 11.68 | 152.7 | 11492 | - | 0 | 0 | 12.38/12.46/15.59 |
| memwal | no | 16 | 3 | 1.07 (1.02-1.13) | 0.86 | 4.00 | 31.30 | 123.0 | 14883 | - | 0 | 0 | 12.18/12.31/12.84 |
| memwal | yes | 1 | 3 | 0.69 (0.65-0.75) | 0.23 | 5.40 | 22.41 | 466.4 | 1453 | 9024 | 0 | 0 | 8.65/9.39/11.35 |
| memwal | yes | 4 | 3 | 1.47 (1.16-1.71) | 0.58 | 11.82 | 77.20 | 469.2 | 2721 | 8988 | 0 | 0 | 10.47/13.57/14.33 |
| memwal | yes | 8 | 3 | 1.95 (1.79-2.27) | 0.82 | 14.96 | 112.10 | 1130.9 | 4076 | 9020 | 0 | 0 | 15.07/15.84/15.06 |
| memwal | yes | 16 | 3 | 2.91 (2.38-3.98) | 1.41 | 23.09 | 154.34 | 423.6 | 5483 | 8908 | 0 | 0 | 13.28/11.45/11.85 |
| memwal-nosnap | no | 1 | 3 | 0.50 (0.44-0.66) | 0.35 | 3.03 | 18.88 | 110.0 | 1981 | - | 0 | 0 | 35.49/42.42/43.06 |
| memwal-nosnap | no | 16 | 3 | 2.79 (2.20-3.45) | 0.84 | 56.44 | 155.71 | 366.2 | 5723 | - | 0 | 0 | 36.86/35.25/37.66 |
| memwal-nosnap | yes | 1 | 3 | 2.70 (1.20-11.50) | 0.57 | 30.77 | 133.78 | 204.2 | 370 | 7541 | 0 | 0 | 36.01/30.74/35.47 |
| memwal-nosnap | yes | 16 | 3 | 13.76 (10.10-19.14) | 3.52 | 168.63 | 283.76 | 477.4 | 1159 | 4882 | 0 | 0 | 40.60/41.10/45.34 |
| ramdisk | no | 1 | 3 | 1.74 (1.70-1.80) | 1.67 | 3.03 | 4.74 | 13.6 | 572 | - | 0 | 0 | 4.01/4.31/4.34 |
| ramdisk | no | 4 | 3 | 1.01 (0.86-1.11) | 0.92 | 2.00 | 5.44 | 45.7 | 3960 | - | 0 | 0 | 11.20/11.07/10.46 |
| ramdisk | no | 8 | 3 | 1.38 (1.32-1.50) | 1.29 | 3.17 | 8.23 | 35.0 | 5791 | - | 0 | 0 | 17.45/15.42/14.89 |
| ramdisk | no | 16 | 3 | 2.42 (2.22-2.56) | 2.09 | 8.98 | 21.73 | 105.0 | 6596 | - | 0 | 0 | 15.12/16.05/17.70 |
| ramdisk | yes | 1 | 3 | 3.90 (3.75-4.18) | 1.74 | 18.44 | 27.00 | 84.9 | 256 | 4032 | 0 | 0 | 4.12/6.17/10.92 |
| ramdisk | yes | 4 | 3 | 6.66 (6.06-8.20) | 4.12 | 32.19 | 52.38 | 176.4 | 600 | 4032 | 0 | 0 | 13.79/15.37/19.12 |
| ramdisk | yes | 8 | 3 | 2.87 (2.59-3.11) | 1.54 | 19.83 | 36.06 | 78.9 | 2782 | 4032 | 0 | 0 | 13.17/12.84/15.39 |
| ramdisk | yes | 16 | 3 | 14.02 (12.31-15.53) | 6.34 | 124.53 | 187.23 | 310.6 | 1137 | 3336 | 0 | 0 | 16.98/21.23/25.58 |

### 4.2 Reading the numbers

- **Ursula-side floor (`ramdisk`, no background).** N=1: mean 1.74, p50 1.67, p99 3.03, p999 4.74. N=4 and N=8 are no slower per commit (1.0–1.4 ms mean), because a lone writer pays wake-up costs that concurrent writers amortise. N=16: mean 2.42, p99 9.0, p999 21.7, at 6 600 commits/s aggregate on loopback.
- **Memory WAL** removes about 1.5 ms (N=1 mean 0.22 ms). The disk-WAL path, meaning journal write, flush syscall and follower persistence before ack, is the larger part of Ursula's local cost even when the flush itself is nearly free.
- **APFS** shows the fsync-bound regime: N=1 mean 12.4 ms (about 3× the 3.9 ms flush, i.e. several serialized flushes on the commit path), and the aggregate saturates at about 210 commits/s at N=16, with mean 75 ms.
- **Background load.** Tails grow sharply. `ramdisk` at N=1 goes to p99 18 ms; at N=16 it goes to mean 14 ms, p99 125 ms and p999 187 ms, at load average 17–25. `memwal` at N=16 goes to p99 23 ms and p999 154 ms. This is CPU contention on a shared 10-core box plus cold flush; on dedicated voters it would be smaller. These cells are the reason the multi-AZ run must include the 50% background generator on separate hosts.
- **By size class** (pooled, no background): `ramdisk` N=1 ≤512 B 1.72 ms, ≤4 KiB 1.79 ms, >4 KiB 2.18 ms. `memwal` 0.22/0.22/0.32. `apfs` 12.37/12.56/13.53. Large tool settles add about 0.4 ms.

### 4.3 Errors and 412s

- 5 112 468 measured matched appends in retained runs: **0 × 412**.
- In the discarded first attempt, one 412 appeared in an N=1 run. A single writer cannot race, so it implies an append that was applied but whose response did not reach the client as success, followed by a retry. The bench does not retry, so the source is unexplained. It was not reproduced in any later run.
- 10 transport errors ("error following redirect"): 2 in `apfs` N=4 r1 and 8 in `apfs` N=16 r1, plus 13 in the discarded RAM-disk N=16 r1. All occurred in the first run after a cluster start, while some groups were still settling leadership. The client received and followed a redirect that failed. Either way the owner sees an unknown outcome and must use the design's read-back confirm (§3.3 outcome policy). Follow-up: check whether `ursulagw` should ever pass a 307 through to clients.

### 4.4 Discarded runs: inline snapshots filled the WAL volume

The first `ramdisk` suite used the default snapshot policy (5 000 logs). Without S3, `build_snapshot` falls back to `InlineSnapshotStore` (`raft_snapshot_inline_fallbacks` = `raft_snapshot_builds`). The pointer is serialized with its bytes as a JSON number array. A group snapshot file was 92 MB, of which `pointer_bytes` was a 30.9 M-element array (about 7.7 MB of real snapshot). Builds averaged about 240 ms (`raft_snapshot_build_ns / raft_snapshot_builds`). Three nodes × 16 groups filled the 6 GiB RAM disk, `wal_disk_pressure` turned on (503s, re-elections), and every RAM-disk cell of that suite ended under disk pressure. Those runs are kept out of all tables. The retained `ramdisk` suite disables snapshots, which makes it the clean measure of the WAL path.

Implications:

- The inline fallback's encoding is about 4× the binary size. That matters only for deployments without an object store.
- Snapshot builds run on the thread-per-core loop, so they surface in p999 (compare `memwal` with the default policy, where 281 builds occurred during the N=16 cell). Production uses the S3 snapshot store but still builds, and JSON streams carry their dense record index into every snapshot until the design's sealing lands. The multi-AZ run must keep the default snapshot policy.

## 5. Results: Pi submit→provider

### 5.1 Measured with the real Pi harness (`ks_latency_pi_probe.ts`)

Values are 10 submits per cell, in ms. "L (Node)" is the per-commit latency as Node sees it, including undici and event-loop time. "line/s" is all commits on the harness's single line during the measured window.

| cluster | bg | N | submit→provider mean | p50 | max | L (Node) mean | p50 | p99 | line/s | load 1m |
|---|---|---|---|---|---|---|---|---|---|---|
| ramdisk | no | 1 | 12.0 | 11.6 | 18.7 | 2.8 | 2.7 | 5.1 | 19 | 24.8 |
| ramdisk | no | 4 | 9.3 | 8.8 | 13.1 | 4.4 | 2.6 | 43.4 | 46 | 23.5 |
| ramdisk | no | 8 | 11.4 | 9.8 | 24.4 | 3.1 | 2.4 | 13.9 | 81 | 30.7 |
| ramdisk | no | 16 | 8.8 | 9.1 | 12.3 | 2.2 | 2.0 | 5.2 | 150 | 19.7 |
| ramdisk | 4 000/s | 1 | 40.0 | 29.9 | 100.7 | 9.2 | 6.0 | 57.3 | 17 | 12.2 |
| ramdisk | 4 000/s | 4 | 20.4 | 18.9 | 50.8 | 7.6 | 4.8 | 48.1 | 44 | 13.1 |
| ramdisk | 4 000/s | 8 | 30.6 | 32.2 | 60.9 | 7.9 | 5.1 | 55.7 | 75 | 19.3 |
| ramdisk | 4 000/s | 16 | 86.9 | 83.5 | 179.1 | 11.4 | 7.2 | 93.4 | 126 | 18.5 |
| memwal | no | 1 | 7.0 | 4.4 | 23.6 | 1.5 | 1.1 | 8.1 | 19 | 21.9 |
| memwal | no | 16 | 5.0 | 4.9 | 9.3 | 1.6 | 1.1 | 11.5 | 151 | 20.3 |
| apfs | no | 1 | 52.0 | 51.2 | 64.2 | 13.2 | 12.8 | 33.6 | 16 | 14.2 |
| apfs | no | 16 | 818.5 | 821.3 | 1089.6 | 12.7 | 11.7 | 39.5 | 78 | 13.1 |

Observations:

- In every cell exactly **3** non-partial commits settled between submit and the provider request. Design §9.2 assumes 4.
- Node-side L is about 1 ms above the Rust bench (2.8 vs 1.7 ms on `ramdisk`, N=1). The owner's HTTP client is part of the budget.
- At N=16 on `ramdisk`, the line carried 150 commits/s (the design's offered load is about 142/s) with submit→provider under 13 ms. Pipelining is not needed when L is around 2–3 ms.
- On `apfs` at N=16 the line saturated at 78 commits/s and submit→provider rose to about 0.8 s. This is the failure mode the design describes for L̄ above about 5.5 ms. The real harness degrades faster than the mean-value model.
- The Pi commit sizes observed were mean 260–460 B, p99 0.5–1.3 KB, max 17 KB. These were text turns with no tool output.

### 5.2 Estimated with the line model from the measured distributions

These are the §9.2 model (MVA, mean only) and the simulation (full measured distribution) for N = 1/4/8/16 Pi conversations on one harness line. The input is the pooled bench samples of one cell. A bench N=1 cell is "this harness alone on the cluster"; a bench N=16 cell is "16 harness lines busy on the cluster".

| L source (bench cell) | L̄ | MVA N=1/4/8/16 | sim mean N=1/4/8/16 | sim p99 N=1/4/8/16 |
|---|---|---|---|---|
| ramdisk, no bg, N=1 | 1.74 | 14.7 / 15.7 / 17.4 / 22.5 | 14.6 / 15.5 / 17.0 / 21.0 | 18.6 / 22.2 / 24.1 / 29.5 |
| ramdisk, no bg, N=16 | 2.42 | 17.6 / 19.2 / 22.0 / 31.1 | 17.7 / 19.1 / 21.8 / 29.8 | 35.3 / 37.7 / 46.5 / 87.7 |
| ramdisk, bg, N=1 | 3.90 | 24.0 / 27.4 / 33.8 / 59.8 | 24.5 / 27.0 / 34.6 / 55.4 | 54.4 / 65.3 / 83.9 / 135.9 |
| ramdisk, bg, N=16 | 14.02 | 71 / 108 / 210 / 632 | 66 / 103 / 194 / 607 | 282 / 398 / 652 / 1247 |
| memwal, no bg, N=1 | 0.22 | 8.3 / 8.6 / 8.9 / 9.6 | 8.3 / 8.5 / 9.0 / 9.7 | 9.6 / 10.9 / 13.3 / 16.9 |
| memwal, bg, N=16 | 2.91 | 19.7 / 21.8 / 25.6 / 38.9 | 19.6 / 24.8 / 29.4 / 42.9 | 121 / 210 / 211 / 265 |
| apfs, no bg, N=1 | 12.43 | 64 / 92 / 171 / 524 | 63 / 90 / 158 / 533 | 87 / 121 / 240 / 628 |
| apfs, bg, N=1 | 26.84 | 139 / 269 / 616 / 1503 | 147 / 262 / 575 / 1546 | 568 / 957 / 1543 / 2925 |

Model against measurement:

- **Below saturation the model is conservative.** For `ramdisk` with no background it predicts 15–22 ms against 9–12 ms measured, and for `memwal` 8–10 ms against 5–7 ms. Most of the gap is the 4th blocking commit, which is 3 in the current Pi. With 3 commits the model gives about 11–17 ms for `ramdisk`. The rest is the 1.1 ms per-commit overhead, which looks smaller with a real network client.
- **At saturation the model is optimistic.** For `apfs` N=16 it predicts 530 ms against 820 ms measured. The FIFO model assumes each partial source re-arms only after its own commit settles and that the blocking commits interleave fairly. The real harness keeps more partials queued ahead of a submit.
- **The model's exit-criterion arithmetic still holds** (N=1 submit→provider ≤ 4·L̄ + 5 ms). On `ramdisk`, 4·2.8 + 5 = 16.2 against 12.0 measured; on `apfs`, 4·13.2 + 5 = 57.8 against 52.0. The N=16 criterion (≥ 130 commits/s and ≤ 100 ms) is met locally on `ramdisk` and `memwal` without background, narrowly missed on `ramdisk` with background (126 commits/s against ≥ 130, at 87 ms), and missed on `apfs`.

## 6. Conclusions

1. Ursula contributes about 2 ms of L on loopback with the disk-WAL code path, with a p99 of 3–9 ms depending on concurrency, when it has the CPU to itself. That leaves about 3 ms of the 5 ms mean budget for production fsync and two cross-AZ round trips. The budget is plausible but unproven.
2. Fsync cost is decisive: at about 12 ms of L the line saturates at N=16 and submit→provider exceeds the target by 8×. The target-volume fsync latency, and how the WAL batches fsyncs across groups under load, must be measured on the real volume.
3. Tail latency under background load is the most likely M0c miss. Locally it is confounded by shared CPU. Snapshot builds on the thread-per-core loop, and cold flush, are the in-process suspects to watch in the multi-AZ run (`raft_snapshot_build_ns`, `cold_flush_*`).
4. `Stream-Record-Match` as a single-writer line is correct under load (0 spurious 412 in 5.1 M appends). Transport errors right after cluster start are real and need the owner's read-back confirm.
5. Pi needs 3 blocking commits per submit, not 4. Design §9.2 should be updated, which tightens the N=1 budget in Ursula's favour.
6. The 50 KB tool settles are not a latency problem (+0.4 ms).

## 7. Multi-AZ gate: exact commands (not run)

These commands follow the repo's OpenTofu + Helm path (`deploy/eks`, `charts/ursula`). They cost money and were **not** executed.

```bash
# 0. Build the bench and probe artefacts for linux/amd64 (or arm64 to match the node group).
cargo build --release -p ursula-bench --target x86_64-unknown-linux-gnu   # or: cross build --release -p ursula-bench --target x86_64-unknown-linux-gnu
IMAGE_TAG=<release tag containing the keyed-streams branch>

# 1. Provision the 3-AZ EKS stack (gp3 by default; set the WAL StorageClass to the target volume, e.g. io2 or local NVMe, in terraform.tfvars).
cd deploy/eks
# edit terraform.tfvars (image tag, region, operator CIDR, WAL StorageClass); see deploy/eks/README.md on main
tofu init && tofu apply
export KUBECONFIG=$PWD/kubeconfig

# 2. Install Ursula: 3 voters across 3 AZs with the S3 cold tier and snapshot store, gateway enabled.
helm install ursula ../../charts/ursula -n ursula --create-namespace \
  -f generated-values.yaml \
  --set global.image.tag=$IMAGE_TAG \
  --set server.replicaCount=3
helm test ursula -n ursula

# 3. Bench pods: one owner pod pinned to a zone, and a background-load pod in another zone.
for z in a b c; do
  kubectl -n ursula run m0c-owner-$z --image=debian:bookworm-slim \
    --overrides="{\"spec\":{\"nodeSelector\":{\"topology.kubernetes.io/zone\":\"${AWS_REGION}${z}\"}}}" \
    --command -- sleep infinity
done
kubectl -n ursula run m0c-bg --image=debian:bookworm-slim \
  --overrides="{\"spec\":{\"nodeSelector\":{\"topology.kubernetes.io/zone\":\"${AWS_REGION}c\"}}}" \
  --command -- sleep infinity
for p in m0c-owner-a m0c-owner-b m0c-owner-c m0c-bg; do
  kubectl -n ursula cp target/x86_64-unknown-linux-gnu/release/ursula-bench $p:/usr/local/bin/ursula-bench
  kubectl -n ursula cp scripts/ks_latency_matrix.sh $p:/ks_latency_matrix.sh
  kubectl -n ursula exec $p -- sh -c 'apt-get update -qq && apt-get install -y -qq python3 procps >/dev/null'
done
GW=http://ursula-gateway.ursula.svc.cluster.local:4437   # gateway Service (see `kubectl -n ursula get svc`)

# 4. Find leader AZs. Run the owner from a zone that hosts no leader for its streams' groups
#    (ursulactl status lists group leaders; with 3 AZs every pod is non-leader for about 2/3 of groups).
kubectl -n ursula port-forward svc/ursula 4437:4437 &
ursulactl status --config cluster-manifest.json > leaders.txt

# 5. Capacity, then 50% background with cold flush on (runs for the whole matrix).
kubectl -n ursula exec m0c-bg -- ursula-bench multi-stream --target $GW --bucket cap --streams 256 --duration-secs 30 --payload-bytes 1024
#    BG = 0.5 × aggregate_ops_per_sec from the line above
kubectl -n ursula exec m0c-bg -- ursula-bench multi-stream --target $GW --bucket bg --streams 256 \
  --rate-per-stream $((BG / 256)) --payload-bytes 1024 --duration-secs 7200 &

# 6. The M0c matrix from each owner zone: N = 1/4/8/16, 3 runs, Pi mix.
for z in a b c; do
  kubectl -n ursula exec m0c-owner-$z -- env KS_RUNS=3 KS_DURATION=60 KS_WARMUP=5 \
    bash /ks_latency_matrix.sh $GW az-$z /results
  kubectl -n ursula cp m0c-owner-$z:/results ./m0c-results-3az-$z
done
python3 scripts/ks_latency_summarize.py ./m0c-results-3az-a   # and -b, -c

# 7. Pi probe from the non-leader owner zone (needs Node 22 and a Pi checkout in the pod):
#    TARGET=$GW N=1|4|8|16 TURNS=50 node --experimental-strip-types --import ./register.mjs ks_latency_pi_probe.ts

# 8. 1-AZ comparison: fresh install (voter placement cannot change in place) with voters, gateway and owner in one zone,
#    then repeat steps 3 to 7 with only the zone-a owner.
helm uninstall ursula -n ursula && kubectl -n ursula delete pvc -l app.kubernetes.io/instance=ursula
helm install ursula ../../charts/ursula -n ursula -f generated-values.yaml --set global.image.tag=$IMAGE_TAG \
  --set-json 'server.scheduling.nodeSelector={"topology.kubernetes.io/zone":"'${AWS_REGION}'a"}' \
  --set-json 'gateway.scheduling.nodeSelector={"topology.kubernetes.io/zone":"'${AWS_REGION}'a"}' \
  --set-json 'server.scheduling.topologySpreadConstraints=[{"maxSkew":3,"topologyKey":"topology.kubernetes.io/zone","whenUnsatisfiable":"ScheduleAnyway","labelSelector":{"matchLabels":{"app.kubernetes.io/instance":"ursula"}}}]'
#    (also change the S3 snapshot/cold prefix, or empty the bucket prefix, so the fresh cluster starts clean)

# 9. Collect node metrics for snapshot and cold-flush attribution, then tear down.
kubectl -n ursula exec ursula-0 -- wget -qO- http://127.0.0.1:4437/__ursula/metrics > metrics-ursula-0.json
helm uninstall ursula -n ursula && tofu destroy
```

Things to record alongside the numbers: the WAL volume type and its `fio --fdatasync=1` latency, inter-AZ ping RTT between pods, which AZ hosted each stream's leader, `raft_snapshot_builds` / `raft_snapshot_build_ns`, and `cold_flush_*` counters.

## 8. Appendix: per-run results

Each row is one 20 s run (3 s warm-up excluded). Latency is in ms. "load 1m" is the 1-minute load average at run start.

| config | bg | N | run | commits/s | mean | p50 | p99 | p999 | max | 412 | errors | load 1m |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| apfs | no | 1 | 1 | 79 | 12.64 | 12.85 | 19.07 | 63.68 | 70.5 | 0 | 0 | 11.56 |
| apfs | no | 1 | 2 | 80 | 12.45 | 12.67 | 19.63 | 26.27 | 26.6 | 0 | 0 | 9.85 |
| apfs | no | 1 | 3 | 82 | 12.20 | 12.57 | 16.69 | 29.31 | 33.4 | 0 | 0 | 7.73 |
| apfs | no | 4 | 1 | 110 | 36.25 | 35.87 | 59.87 | 107.58 | 128.3 | 0 | 2 | 3.93 |
| apfs | no | 4 | 2 | 108 | 37.09 | 36.32 | 54.59 | 64.13 | 76.6 | 0 | 0 | 3.34 |
| apfs | no | 4 | 3 | 105 | 37.91 | 36.99 | 54.59 | 61.05 | 64.8 | 0 | 0 | 4.14 |
| apfs | no | 8 | 1 | 154 | 51.67 | 49.79 | 94.78 | 137.47 | 139.8 | 0 | 0 | 3.83 |
| apfs | no | 8 | 2 | 150 | 53.27 | 52.13 | 88.25 | 102.59 | 122.4 | 0 | 0 | 4.94 |
| apfs | no | 8 | 3 | 154 | 51.78 | 51.20 | 84.80 | 99.45 | 104.2 | 0 | 0 | 4.15 |
| apfs | no | 16 | 1 | 201 | 79.10 | 77.63 | 134.78 | 169.34 | 196.5 | 0 | 8 | 4.23 |
| apfs | no | 16 | 2 | 214 | 74.41 | 72.25 | 136.45 | 228.74 | 259.5 | 0 | 0 | 3.97 |
| apfs | no | 16 | 3 | 218 | 73.23 | 72.25 | 118.33 | 143.49 | 144.0 | 0 | 0 | 3.96 |
| apfs | yes | 1 | 1 | 34 | 28.99 | 13.19 | 285.95 | 328.45 | 328.4 | 0 | 0 | 6.70 |
| apfs | yes | 1 | 2 | 38 | 26.20 | 12.86 | 262.40 | 291.84 | 291.8 | 0 | 0 | 5.06 |
| apfs | yes | 1 | 3 | 39 | 25.57 | 12.81 | 234.24 | 274.43 | 274.4 | 0 | 0 | 5.09 |
| apfs | yes | 4 | 1 | 57 | 69.88 | 41.34 | 292.86 | 320.51 | 329.5 | 0 | 0 | 4.47 |
| apfs | yes | 4 | 2 | 56 | 70.88 | 46.91 | 245.76 | 267.26 | 269.6 | 0 | 0 | 4.69 |
| apfs | yes | 4 | 3 | 58 | 69.28 | 43.74 | 268.80 | 294.91 | 304.1 | 0 | 0 | 4.23 |
| apfs | yes | 8 | 1 | 65 | 122.14 | 71.36 | 373.76 | 421.12 | 438.3 | 0 | 0 | 4.54 |
| apfs | yes | 8 | 2 | 58 | 137.67 | 75.90 | 404.22 | 441.09 | 481.8 | 0 | 0 | 4.95 |
| apfs | yes | 8 | 3 | 47 | 167.38 | 89.09 | 472.57 | 631.29 | 631.3 | 0 | 0 | 4.34 |
| apfs | yes | 16 | 1 | 83 | 193.00 | 115.65 | 481.02 | 552.96 | 561.7 | 0 | 0 | 4.11 |
| apfs | yes | 16 | 2 | 83 | 189.53 | 113.15 | 571.90 | 650.75 | 677.9 | 0 | 0 | 3.92 |
| apfs | yes | 16 | 3 | 80 | 195.88 | 134.91 | 567.81 | 653.31 | 660.5 | 0 | 0 | 3.99 |
| memwal | no | 1 | 1 | 4872 | 0.20 | 0.19 | 0.34 | 0.83 | 70.6 | 0 | 0 | 12.04 |
| memwal | no | 1 | 2 | 3989 | 0.25 | 0.21 | 0.70 | 1.96 | 59.1 | 0 | 0 | 9.91 |
| memwal | no | 1 | 3 | 4634 | 0.21 | 0.19 | 0.48 | 1.15 | 61.9 | 0 | 0 | 8.91 |
| memwal | no | 4 | 1 | 9279 | 0.43 | 0.39 | 0.81 | 5.33 | 78.7 | 0 | 0 | 10.57 |
| memwal | no | 4 | 2 | 8241 | 0.48 | 0.43 | 1.03 | 6.13 | 71.4 | 0 | 0 | 10.78 |
| memwal | no | 4 | 3 | 8939 | 0.45 | 0.41 | 0.90 | 4.30 | 72.4 | 0 | 0 | 10.63 |
| memwal | no | 8 | 1 | 11921 | 0.67 | 0.57 | 1.68 | 9.94 | 152.8 | 0 | 0 | 12.38 |
| memwal | no | 8 | 2 | 11162 | 0.71 | 0.59 | 2.10 | 11.68 | 109.0 | 0 | 0 | 12.46 |
| memwal | no | 8 | 3 | 11392 | 0.70 | 0.58 | 2.16 | 13.17 | 89.0 | 0 | 0 | 15.59 |
| memwal | no | 16 | 1 | 15596 | 1.02 | 0.85 | 3.18 | 20.94 | 100.2 | 0 | 0 | 12.18 |
| memwal | no | 16 | 2 | 14183 | 1.13 | 0.89 | 4.43 | 44.29 | 123.0 | 0 | 0 | 12.31 |
| memwal | no | 16 | 3 | 14869 | 1.07 | 0.85 | 4.55 | 27.58 | 116.5 | 0 | 0 | 12.84 |
| memwal | yes | 1 | 1 | 1545 | 0.65 | 0.23 | 5.24 | 15.25 | 359.4 | 0 | 0 | 8.65 |
| memwal | yes | 1 | 2 | 1329 | 0.75 | 0.23 | 5.74 | 48.19 | 466.4 | 0 | 0 | 9.39 |
| memwal | yes | 1 | 3 | 1486 | 0.67 | 0.23 | 5.39 | 21.77 | 398.1 | 0 | 0 | 11.35 |
| memwal | yes | 4 | 1 | 3420 | 1.16 | 0.58 | 7.59 | 54.72 | 229.0 | 0 | 0 | 10.47 |
| memwal | yes | 4 | 2 | 2415 | 1.65 | 0.60 | 14.12 | 81.47 | 343.8 | 0 | 0 | 13.57 |
| memwal | yes | 4 | 3 | 2327 | 1.71 | 0.57 | 15.42 | 100.54 | 469.2 | 0 | 0 | 14.33 |
| memwal | yes | 8 | 1 | 4298 | 1.86 | 0.90 | 12.26 | 81.02 | 208.9 | 0 | 0 | 15.07 |
| memwal | yes | 8 | 2 | 4451 | 1.79 | 0.78 | 14.28 | 112.13 | 394.5 | 0 | 0 | 15.84 |
| memwal | yes | 8 | 3 | 3481 | 2.27 | 0.78 | 19.30 | 191.49 | 1131.5 | 0 | 0 | 15.06 |
| memwal | yes | 16 | 1 | 6698 | 2.38 | 1.24 | 17.04 | 93.44 | 185.2 | 0 | 0 | 13.28 |
| memwal | yes | 16 | 2 | 5745 | 2.78 | 1.32 | 22.21 | 150.78 | 423.7 | 0 | 0 | 11.45 |
| memwal | yes | 16 | 3 | 4007 | 3.98 | 1.94 | 29.74 | 226.56 | 419.1 | 0 | 0 | 11.85 |
| memwal-nosnap | no | 1 | 1 | 1510 | 0.66 | 0.36 | 6.36 | 33.47 | 110.1 | 0 | 0 | 35.49 |
| memwal-nosnap | no | 1 | 2 | 2237 | 0.44 | 0.29 | 2.57 | 14.50 | 101.5 | 0 | 0 | 42.42 |
| memwal-nosnap | no | 1 | 3 | 2196 | 0.45 | 0.38 | 1.74 | 14.44 | 67.1 | 0 | 0 | 43.06 |
| memwal-nosnap | no | 16 | 1 | 7270 | 2.20 | 0.86 | 39.45 | 118.02 | 172.3 | 0 | 0 | 36.86 |
| memwal-nosnap | no | 16 | 2 | 5282 | 3.01 | 0.86 | 60.80 | 161.41 | 260.6 | 0 | 0 | 35.25 |
| memwal-nosnap | no | 16 | 3 | 4617 | 3.45 | 0.79 | 81.73 | 183.68 | 366.3 | 0 | 0 | 37.66 |
| memwal-nosnap | yes | 1 | 1 | 828 | 1.20 | 0.43 | 9.94 | 19.58 | 37.2 | 0 | 0 | 36.01 |
| memwal-nosnap | yes | 1 | 2 | 196 | 5.10 | 3.10 | 45.98 | 121.66 | 134.7 | 0 | 0 | 30.74 |
| memwal-nosnap | yes | 1 | 3 | 87 | 11.50 | 3.20 | 145.15 | 203.52 | 204.3 | 0 | 0 | 35.47 |
| memwal-nosnap | yes | 16 | 1 | 1578 | 10.10 | 2.57 | 146.94 | 220.29 | 474.4 | 0 | 0 | 40.60 |
| memwal-nosnap | yes | 16 | 2 | 832 | 19.14 | 4.35 | 202.24 | 318.21 | 456.4 | 0 | 0 | 41.10 |
| memwal-nosnap | yes | 16 | 3 | 1067 | 14.98 | 4.41 | 162.30 | 291.84 | 477.4 | 0 | 0 | 45.34 |
| ramdisk | no | 1 | 1 | 574 | 1.74 | 1.66 | 3.00 | 4.10 | 13.6 | 0 | 0 | 4.01 |
| ramdisk | no | 1 | 2 | 554 | 1.80 | 1.74 | 3.03 | 4.76 | 8.4 | 0 | 0 | 4.31 |
| ramdisk | no | 1 | 3 | 587 | 1.70 | 1.58 | 3.10 | 4.87 | 9.4 | 0 | 0 | 4.34 |
| ramdisk | no | 4 | 1 | 3603 | 1.11 | 1.04 | 2.01 | 4.32 | 15.9 | 0 | 0 | 11.20 |
| ramdisk | no | 4 | 2 | 3643 | 1.09 | 1.02 | 2.16 | 6.43 | 30.9 | 0 | 0 | 11.07 |
| ramdisk | no | 4 | 3 | 4634 | 0.86 | 0.82 | 1.72 | 5.13 | 45.7 | 0 | 0 | 10.46 |
| ramdisk | no | 8 | 1 | 5336 | 1.50 | 1.37 | 3.45 | 8.22 | 35.0 | 0 | 0 | 17.45 |
| ramdisk | no | 8 | 2 | 6054 | 1.32 | 1.25 | 2.69 | 8.04 | 19.2 | 0 | 0 | 15.42 |
| ramdisk | no | 8 | 3 | 5983 | 1.33 | 1.26 | 2.96 | 8.46 | 15.8 | 0 | 0 | 14.89 |
| ramdisk | no | 16 | 1 | 7204 | 2.22 | 2.02 | 6.22 | 14.59 | 70.7 | 0 | 0 | 15.12 |
| ramdisk | no | 16 | 2 | 6245 | 2.56 | 2.16 | 10.51 | 25.77 | 87.7 | 0 | 0 | 16.05 |
| ramdisk | no | 16 | 3 | 6339 | 2.52 | 2.13 | 10.28 | 22.72 | 105.0 | 0 | 0 | 17.70 |
| ramdisk | yes | 1 | 1 | 239 | 4.18 | 1.92 | 18.43 | 25.14 | 33.3 | 0 | 0 | 4.12 |
| ramdisk | yes | 1 | 2 | 262 | 3.81 | 1.72 | 18.75 | 27.04 | 31.9 | 0 | 0 | 6.17 |
| ramdisk | yes | 1 | 3 | 266 | 3.75 | 1.65 | 18.59 | 30.00 | 85.0 | 0 | 0 | 10.92 |
| ramdisk | yes | 4 | 1 | 654 | 6.11 | 3.80 | 27.34 | 38.53 | 74.1 | 0 | 0 | 13.79 |
| ramdisk | yes | 4 | 2 | 487 | 8.20 | 6.05 | 39.71 | 72.70 | 176.5 | 0 | 0 | 15.37 |
| ramdisk | yes | 4 | 3 | 659 | 6.06 | 3.44 | 30.21 | 45.44 | 56.6 | 0 | 0 | 19.12 |
| ramdisk | yes | 8 | 1 | 3078 | 2.59 | 1.60 | 12.04 | 22.08 | 40.5 | 0 | 0 | 13.17 |
| ramdisk | yes | 8 | 2 | 2703 | 2.96 | 1.41 | 23.87 | 39.87 | 58.8 | 0 | 0 | 12.84 |
| ramdisk | yes | 8 | 3 | 2566 | 3.11 | 1.61 | 23.28 | 37.92 | 79.0 | 0 | 0 | 15.39 |
| ramdisk | yes | 16 | 1 | 1295 | 12.31 | 6.31 | 112.70 | 186.62 | 273.4 | 0 | 0 | 16.98 |
| ramdisk | yes | 16 | 2 | 1087 | 14.65 | 6.19 | 125.82 | 183.42 | 310.8 | 0 | 0 | 21.23 |
| ramdisk | yes | 16 | 3 | 1027 | 15.53 | 6.58 | 131.46 | 189.44 | 309.8 | 0 | 0 | 25.58 |

Raw samples (`*.samples`, one `<latency_us> <bytes>` per commit), per-run JSON and node-1 metrics were kept locally under `/private/tmp/ks-m0c/results` and are not committed (about 60 MB). Regenerate them with:

```bash
cargo build --release --bin ursula -p ursula && cargo build --release -p ursula-bench
dev=$(hdiutil attach -nomount ram://12582912 | awk '{print $1}'); diskutil erasevolume HFS+ ksm0cram $dev
R=/tmp/ks-m0c/results
KS_SNAPSHOT_LOGS=100000000 scripts/ks_latency_suite.sh ramdisk disk /Volumes/ksm0cram/ram 15440 4000 $R
scripts/ks_latency_suite.sh memwal memory /tmp/ks-m0c/mem 15420 9000 $R
scripts/ks_latency_suite.sh apfs disk /tmp/ks-m0c/apfs 15400 110 $R
python3 scripts/ks_latency_summarize.py $R
cat $R/ramdisk-bg0-n1-r*.samples > pool.samples && python3 scripts/ks_latency_line_model.py sim pool.samples
hdiutil detach $dev
```
