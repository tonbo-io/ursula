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
- `full-resident` (M1; `auto` against a node without keyed-state): every record replayed from 0 at open.

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

`test/stack/` holds the stack the e2e suite and the drills share: process control, a MinIO launcher and a
minimal SigV4 S3 client, a TCP fault proxy (S3 and indexer outages, blue/green cutover), the cluster
(nodes, gateway, indexer, feature-level raise) and a fleet of live owners that captures every append for
byte-for-byte verification.

```ts
import { httpTransports, UrsulaStorage } from "@tonbo-io/pi-durable-ursula";
const { log, keyedState } = httpTransports({ baseUrl: "http://127.0.0.1:4437", stream: "pi/harness-1", token });
const storage = await UrsulaStorage.open({ log, keyedState, mode: "fail-if-active" }); // bounded when keyed-state is served
```
