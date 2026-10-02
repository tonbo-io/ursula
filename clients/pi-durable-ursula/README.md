# @tonbo-io/pi-durable-ursula

Pi Durable `Storage` on an Ursula keyed stream: one Pi harness is one `application/json; profile=keyed-batch-v1`
stream, one Pi commit is one `keyed-batch-v1` record, and Pi's `Seq` is the record ordinal. The design is
`docs/architecture/keyed-streams-pi-durable.md`.

This is the M1 owner core: tuple layer, key families, commit planner, Pi read plans, the commit outcome policy
with read-back, open/claim in `fail-if-active` (default) and `fence` modes, close markers, and poison semantics,
over a full-resident state store (every record replayed from 0 at open). It talks to Ursula through the
`LogTransport` and `KeyedStateTransport` interfaces in `src/transport.ts`, implemented over `fetch` in
`src/http.ts`. `src/fake/` is an in-memory Ursula with fold semantics, `Stream-Record-Match`, and fault injection.

```sh
npm ci
npm run typecheck
npm test          # FUZZ_TABLE_TRIALS / FUZZ_DOC_TRIALS scale the differential fuzzers

# End to end against a real single-node ursula (memory engine, free port), spawned by the suite:
cargo build --release -p ursula --bin ursula
URSULA_BIN=../../target/release/ursula npm run test:e2e   # E2E_REQUIRE_KEYED=1 requires keyed-batch-v1
```

```ts
import { httpTransports, UrsulaStorage } from "@tonbo-io/pi-durable-ursula";
const { log, keyedState } = httpTransports({ baseUrl: "http://127.0.0.1:4437", stream: "pi/harness-1", token });
const storage = await UrsulaStorage.open({ log, keyedState, mode: "fail-if-active" });
```
