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
```

```ts
import { httpTransports, UrsulaStorage } from "@tonbo-io/pi-durable-ursula";
const { log, keyedState } = httpTransports({ baseUrl: "http://127.0.0.1:4437", stream: "pi/harness-1", token });
const storage = await UrsulaStorage.open({ log, keyedState, mode: "fail-if-active" }); // bounded when keyed-state is served
```
