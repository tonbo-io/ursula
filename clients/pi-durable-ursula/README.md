# @tonbo-io/pi-durable-ursula

Pi Durable `Storage` on an Ursula keyed stream: one Pi harness is one `application/json; profile=keyed-batch-v1`
stream, one Pi commit is one `keyed-batch-v1` record, and Pi's `Seq` is the record ordinal. The design is
`docs/architecture/keyed-streams-pi-durable.md`.

This is the M1 owner core: tuple layer, key families, commit planner, Pi read plans, the commit outcome policy
with read-back, open/claim in `fail-if-active` (default) and `fence` modes, close markers, and poison semantics,
over a full-resident state store (every record replayed from 0 at open). It talks to Ursula through the
`LogTransport` and `KeyedStateTransport` interfaces in `src/transport.ts`, implemented over `fetch` in
`src/http.ts`. `src/fake/` is an in-memory Ursula with fold semantics, `Stream-Record-Match`, and fault injection.

`src/local-store/` is the bounded owner state of M3 (§7.2–§7.5), not yet wired into `UrsulaStorage`:
`LocalStore` keeps the overlay of records `[E, tail)` plus a range cache of materialized `state(tail)` rows
(skip list), merges keyed-state pages at `D_resp ≥ E` by replaying the overlay, serves new IDs locally via
the complete-at-mint fresh floor, and evicts LRU ranges that no running read has pinned.

```sh
npm ci
npm run typecheck
npm test          # FUZZ_TABLE_TRIALS / FUZZ_DOC_TRIALS scale the differential fuzzers;
                  # LOCAL_STORE_MODEL_CASES (default 10^4; 10^5 for M0e) / LOCAL_STORE_PI_CASES scale the LocalStore model tests

# End to end against a real single-node ursula (memory engine, free port), spawned by the suite:
cargo build --release -p ursula --bin ursula
URSULA_BIN=../../target/release/ursula npm run test:e2e   # E2E_REQUIRE_KEYED=1 requires keyed-batch-v1
```

```ts
import { httpTransports, UrsulaStorage } from "@tonbo-io/pi-durable-ursula";
const { log, keyedState } = httpTransports({ baseUrl: "http://127.0.0.1:4437", stream: "pi/harness-1", token });
const storage = await UrsulaStorage.open({ log, keyedState, mode: "fail-if-active" });
```
