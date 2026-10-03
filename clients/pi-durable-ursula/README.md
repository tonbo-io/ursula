# @tonbo-io/pi-durable-ursula

Pi Durable `Storage` on an Ursula keyed stream: one Pi harness is one `application/json; profile=keyed-batch-v1`
stream, one Pi commit is one `keyed-batch-v1` record, and Pi's `Seq` is the record ordinal. The design is
`docs/architecture/keyed-streams-pi-durable.md`.

The owner implements the tuple layer, key families, commit planner, Pi read plans, the commit outcome policy
with read-back, open/claim in `fail-if-active` (default) and `fence` modes, close markers, and poison semantics.
It talks to Ursula through the `LogTransport` and `KeyedStateTransport` interfaces in `src/transport.ts`,
implemented over `fetch` in `src/http.ts`. `src/fake/` is an in-memory Ursula with fold semantics,
`Stream-Record-Match`, fault injection, and an indexer model (`normal`, `paused`, `aggressive`).

State sits behind the `StateStore` contract (`stateStore` option):

- `bounded` (M3; the `auto` default when the node advertises `keyed-state-v1`): `src/local-store/` keeps the
  overlay of records `[E, tail)` plus a range cache of materialized `state(tail)` rows. Open reads `m/` through
  keyed-state, replays `[D, N0)` into the overlay, preloads the live set (`src/bounded.ts`), then claims.
  Reads that miss the cache fetch keyed-state at `min_through_record = E` and merge the page by replaying the
  overlay; new IDs are served locally by the complete-at-mint fresh floor. A background flush loop
  (`src/flush.ts`) raises `E` and detects takeovers. Budgets: `cacheBudgetBytes` (64 MiB), `overlayCapBytes`
  (256 MiB). Session-line reads retry transient failures for 30 s, then poison; flush-waits retry forever.
  Eviction (§7.5) drops values over 1 MiB first, then shrinks least-recently-used ranges from their cold
  end (away from the key last read), then raises `F_fresh`; adjacent unpinned ranges are coalesced. The
  overlay raises an `overlay-size` alert (`onAlert`) at 64 MiB (`overlayAlertBytes`).
- `full-resident` (M1; `auto` against a node without keyed-state): every record replayed from 0 at open.

Owner metrics (§7.6): `storage.metrics()` returns Session-line and open remote reads, their retries and
latency (mean, p50, p99), pinned (overlay) and cache bytes, `E`, flush-wait counts, and poison, fence,
contention, refusal and claim-timeout counts. Pass one `OwnerMetrics` as `metrics` to every open of a host
to aggregate them, including open-time events such as `OwnershipContention`.

All deadlines and backoff sleeps, including the flush loop's, run on the injectable `clock`; tests that
inject faults use a virtual clock (`test/helpers.ts`), so they never sleep on the wall clock.

```sh
npm ci
npm run typecheck
npm test          # FUZZ_TABLE_TRIALS / FUZZ_DOC_TRIALS scale the differential fuzzers;
                  # LOCAL_STORE_MODEL_CASES (default 10^4; 10^5 for M0e) / LOCAL_STORE_PI_CASES scale the LocalStore model tests

# End to end against a real single-node ursula (memory engine, free port), spawned by the suite:
cargo build --release -p ursula --bin ursula
URSULA_BIN=../../target/release/ursula npm run test:e2e   # requires keyed-batch-v1; E2E_REQUIRE_KEYED=0 for an older node
# The same suite on S3 (MinIO: URSULA_S3_ENDPOINT, or a local `minio` / MINIO_BIN) and/or on a 3-node cluster behind the gateway:
E2E_S3=1 URSULA_BIN=../../target/release/ursula npm run test:e2e
E2E_NODES=3 E2E_S3=1 URSULA_BIN=../../target/release/ursula npm run test:e2e

# The M4 drills (docs/architecture/keyed-streams-drills.md); each starts its own stack. DRILL_OUTAGE_S
# (default 600), DRILL_OWNERS, DRILL_SETTLE_S, DRILL_RECOVERY_S, DRILL_COMMIT_DEADLINE_MS shorten them;
# the rolling-upgrade drill needs main's binary (scripts/ks_build_old_ursula.sh) in URSULA_OLD_BIN.
URSULA_BIN=../../target/release/ursula URSULA_OLD_BIN=../../target/ks-old/e6d8d70/ursula npm run test:drills
```

### Manual tools (not run in CI)

These are kept as documented tools and run by hand; no workflow runs them.

```sh
# M3 performance gates (§10 M3, §11.10, test/e2e/perf/open.perf.ts) against the e2e stack; several minutes:
URSULA_BIN=../../target/release/ursula PERF_OUT=perf.json npm run perf:manual   # PERF_GATES=0 reports without asserting

# Keyed-streams soak (test/soak/, 3 nodes + gateway + keyed indexer on MinIO, a mixed Pi population);
# planned as a run on AWS against real S3. SOAK_* knobs, see scripts/ks_soak.sh:
SOAK_DURATION_S=3600 ../../scripts/ks_soak.sh
```

`test/stack/` holds the stack the e2e suite and the drills share: process control, a MinIO launcher and a
minimal SigV4 S3 client, a TCP fault proxy (S3 and indexer outages, blue/green cutover), the cluster
(nodes, gateway, indexer, feature-level raise) and a fleet of live owners that captures every append for
byte-for-byte verification.

```ts
import { httpTransports, UrsulaStorage } from "@tonbo-io/pi-durable-ursula";
const { log, keyedState } = httpTransports({ baseUrl: "http://127.0.0.1:4437", stream: "pi/harness-1", token });
const storage = await UrsulaStorage.open({ log, keyedState, mode: "fail-if-active" }); // bounded when keyed-state is served
```

## M3 benchmark results

`npm run perf:manual` (`test/e2e/perf/open.perf.ts`) on 2026-10-02, after the indexer read-path fix (decoded part
footers, page indexes and verified tails cached per part and shared by the pod, write cache footers decoded once,
memory-hit range blocks not re-hashed, single-block reads without copying): Apple Silicon laptop, 10 cores, under
load from other builds; single-node memory-engine `ursula` plus the keyed indexer on the same machine, 30
interleaved samples per scenario after one warm-up open. Histories are written by a real Pi Harness (two text
turns and one tool turn per three, a `pi.reset` every 30 turns) and bulk-loaded. Harness-level open is
`UrsulaStorage.open` + `Harness.open` + root + `resume()` + first `taskGraph` + first `viewState`; first submit
is submit → provider request.

| Scenario | Records | Open p50 / p99 (ms) | First submit p50 (ms) | Remote reads at open | keyed-state point read p50 (ms) |
|---|---|---|---|---|---|
| 1k records | 1,004 | 5.7 / 8.9 | 1.5 | 21 | 0.26 |
| 100k records | 100,005 | 5.7 / 8.1 | 1.4 | 21 | 0.21 |
| 10 conversations | 6,001 | 5.0 / 8.6 | 1.2 | 21 | 0.24 |
| 1,000 conversations | 5,997 | 4.2 / 7.2 | 1.3 | 21 | 0.17 |

Every gate passes: open p50 ≤ 250 ms and p99 ≤ 1 s, first submit p50 ≤ 300 ms, 1,000 vs 10 conversations 0.84×,
100k vs 1k records 1.00× (gate 1.2×). The owner issues the same 21 keyed-state reads at open in both cases, at
most about four round trips deep, with `viewState` and the first turn fully local (0 remote reads). Before the
fix the ratio was 1.95×: keyed-state point reads took 0.32 ms at 1k records and 1.04 ms at 100k, because the
indexer fetched, hashed and decoded each part's footer and page index on every read. The indexer-side
microbenchmark is `cargo bench -p ursula-index --bench keyed_point_read`.

Zero remote reads on the Session line for steady-state text and tool turns holds on the fake under every
indexer mode (`test/bounded-harness.test.ts`) and on the real stack (`test/e2e/steady-state.e2e.ts`), checked
both by an instrumented transport and by `metrics().sessionLineRemoteReads`.
