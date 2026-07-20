# Codebase Size Reduction Plan

## Objective

Shrink the Ursula workspace from ~93.9k lines of Rust (66.4k production, 27.5k
test) to roughly three quarters of its current size (~70k lines, a ~23.5k line
cut) without losing protocol coverage, DST determinism, or shipped
functionality that the project still wants.

The project is in the 0.x prototype phase: hard breaking changes are preferred
over compatibility shims, and internal wire-format changes are acceptable
because every deployment upgrades atomically.

This document records the survey findings (2026-07), the reduction levers in
priority order, and the staged execution plan. Each stage lands as independent
semantic commits so any lever can be reverted or deferred on its own.

## Survey summary

Per-crate baseline (lines of Rust, production / test-like files):

| Crate                | Prod   | Test   | Dominant reduction lever                     |
| -------------------- | ------ | ------ | -------------------------------------------- |
| ursula-runtime       | 14,564 | 6,028  | metrics + actor-dispatch boilerplate, WAL    |
| ursula-sim           | 16,662 | 1,823  | minimizer dedup, binary merge, http helpers  |
| ursula               | 6,276  | 8,470  | tests.rs request helpers, DST-overlap tests  |
| ursula-raft          | 11,173 | 3,100  | hand-written proto codecs, test fixtures     |
| ursula-stream        | 5,060  | 4,559  | state-machine test fixtures                  |
| ursula-index         | 4,884  | 1,514  | none (shipped service; document it instead)  |
| others (8 crates)    | 7,774  | 1,968  | config table tests, shared shutdown helpers  |

Three structural findings dominate everything else:

1. **One domain model spelled out five to six times.** `StreamCommand` /
   `StreamResponse` / `StreamErrorCode` exist natively in `ursula-stream`, are
   mirrored as `GroupWriteCommand` / `GroupWriteResponse` in `ursula-runtime`,
   exist a third time as prost-generated types, and are bridged by
   hand-written encode/decode in `ursula-raft/src/types.rs:124-298`,
   `ursula-raft/src/codec.rs` (1,173 lines), and per-variant translation in
   `ursula-runtime/src/engine/in_memory.rs:140-760`. The openraft envelope
   types get the same treatment in `ursula-raft/src/log_store/mod.rs:137-465`.
2. **Per-operation boilerplate in the actor pipeline and metrics.** Every
   runtime operation threads four hand-written sites (`runtime.rs` method,
   `GroupCommand` variant, reject arm, dispatch arm), and `metrics.rs`
   (1,222 lines) repeats a four-section pattern per metric. Both are
   macro-collapsible with zero behavior change.
3. **Test boilerplate and DST-overlapping coverage.** 178 hand-rolled
   `Request::builder()` blocks in `ursula/src/tests.rs`, 61 more in
   `ursula-sim/src/madsim_harness/http.rs`, ten byte-identical `shrink_*`
   functions in the sim minimizer, five sim binaries re-declaring the same
   artifact structs and arg parsing, and a ~3.1k-line multi-node integration
   block in `ursula/src/tests.rs` that is a strictly weaker duplicate of
   madsim DST scenarios.

Negative findings, recorded so nobody re-litigates them:

- **ursula-index is a shipped service**, deployed by the Dockerfile and Helm
  chart with sustained commit activity. It shares only ~50 lines of S3
  operator-builder code with the runtime cold path. It is not a deletion
  target; it should be added to the CLAUDE.md crate table.
- **ursula-gateway does not duplicate the main server** (reqwest reverse
  proxy vs in-process axum handlers) and **ursula-control is not a duplicate
  API client** (it is the meta-Raft replicated state machine).
- **There is almost no compatibility debt**: zero `#[deprecated]` items, four
  real `#[allow(dead_code)]`s, single-source protos via symlinks, and no dead
  `cfg` dual paths beyond the sanctioned `cfg(madsim)`.

## Stages

Estimated cuts are ranges from the survey; actual numbers are recorded per
commit as stages land.

### Stage 1 — mechanical shrink, zero behavior change (~8-10k lines)

No functional or wire-format change. Every item keeps existing coverage and
public serialized names intact.

1. **ursula-sim minimizer dedup.** Replace the ten identical
   `shrink_*_schedule` loops and twelve near-identical `minimize_*` drivers in
   `bin/ursula-sim-minimize.rs` with one generic shrink loop and one generic
   minimize driver parameterized by the scenario-specific candidate-schedule
   function. The mutation operators stay untouched.
2. **ursula-sim binary merge.** Collapse the five binaries (`smoke`,
   `replay`, `minimize`, `record`, `assert-shape`) into one `ursula-sim` CLI
   with subcommands. Move the per-binary re-declared artifact structs
   (`FailedSeedArtifact`, `StableTraceArtifact`, ...) and helpers
   (`panic_payload_to_string`, `stable_trace`, ...) into the library. Replace
   the 33-variant `ScheduleKind` longhand with a static name→generator table.
   Artifact JSON field names stay identical.
3. **Dead scaffolding.** Delete the empty `faults/`, `invariants/`,
   `scenarios/`, `workloads/` placeholder modules in ursula-sim.
4. **HTTP request helpers.** One shared request/assert helper set for the 178
   `Request::builder()` blocks in `ursula/src/tests.rs` and the 61 in
   `ursula-sim/src/madsim_harness/http.rs`. Every status and header assertion
   stays visible at the call site.
5. **metrics.rs macro.** Generate `RuntimeMetricsInner` fields, `new()`,
   `snapshot()` collectors, and `RuntimeMetricsSnapshot` from one declarative
   metric manifest (name, scope, aggregation). Serialized field names are
   preserved exactly.
6. **thiserror migration.** Replace hand-rolled `Display`/`Error` impls in
   `ursula-runtime`, `ursula-stream`, `ursula-raft`, and `ursula-shard` with
   `thiserror` derives, keeping message strings identical. Arms with
   conditional formatting stay hand-written.
7. **Test fixtures.** Shared fixture builders for the repeated inline setup:
   `StreamStateMachine` setup (60+ sites in ursula-stream), cluster/placement
   setup in ursula-raft tests, `ColdStore::memory()` / `RuntimeConfig` /
   spawn helpers in ursula-runtime tests, table-driven parse tests in
   ursula-config, and the four-tier `spawn_static_grpc_test_node*` cascade in
   ursula.
8. **Small dedups.** Shared shutdown-signal/serve helper for the three
   binaries; gateway switched to `ursula_observability::init`; generic
   leader-forwarding in `ursula-raft/src/forward.rs`; gRPC client-call helper
   in `grpc.rs`; `LeadershipShedState` on the `bitflags` crate.

### Stage 2 — structural unification (~4-5k lines)

Behavior-preserving for clients, but changes internal APIs and the inter-node
wire format. Requires a full DST corpus pass before merge.

1. **Single canonical command/response/error model.** Unify
   `GroupWriteCommand`/`GroupWriteResponse` with `StreamCommand` /
   `StreamResponse`; make one representation the wire type so the hand-written
   codec mirrors in `ursula-raft/src/codec.rs`, `types.rs`, and the
   per-variant translation in `engine/in_memory.rs` collapse.
2. **Serde-carried Raft envelope.** Carry openraft's own serde-capable types
   in the Raft RPC envelope instead of bespoke proto mirrors, deleting
   `log_store/mod.rs:137-465` and the matching `.proto` messages. Trade-off:
   couples the inter-node wire format to Rust type layout, which is
   acceptable while all nodes upgrade together.
3. **Actor-dispatch macro.** Generate the `GroupCommand` variant, reject arm,
   dispatch arm, and `ShardRuntime` method per operation. Hot-path change;
   gate on DST re-validation and the append/apply benchmarks.
4. **Cold-flush API trim.** Remove single-candidate `plan_next_cold_flush`
   (batch with `max = 1` subsumes it) and fold the `*_with_cold_admission`
   variants into an `Option` parameter.

### Stage 3 — feature retirement (product decisions, ~2-3k lines)

Each item deletes a real capability and needs an explicit go/no-go:

| Decision                                             | Est. lines | Cost                                        |
| ---------------------------------------------------- | ---------- | ------------------------------------------- |
| Drop WAL persistence backend (`engine/wal.rs`)       | ~1,350     | loses single-node WAL mode (Raft covers it) |
| Fold meta-Raft into group Raft or drop it            | ~700-1,000 | architectural; migrates control-plane state |
| Drop `/v1/stream` S2-compat routes + bench `ApiStyle::S2` | ~250  | loses S2-compatible API surface             |
| Drop `LocalSnapshotStore` backend                    | ~150       | loses `snapshot.backend = local`            |
| Drop JSON/YAML config formats (keep TOML)            | ~160       | loses two config formats and two deps       |
| Retire `InProcessRaft` fault-injection network       | ~450       | requires confirming equivalent DST coverage |

### Stage 4 — test-surface pruning (coverage judgment, ~4-5k lines)

1. Delete multi-node integration tests in `ursula/src/tests.rs` whose
   scenario is provably covered by a named madsim DST scenario. Each deletion
   must cite the covering scenario in the commit message; HTTP wire-format
   assertions stay.
2. Merge the ~33 near-duplicate corruption/failover seed families in the sim
   smoke corpus behind one parameterized `CorruptionKind` axis.
3. Table-drive the eleven `madsim_*_strict_replay_*_probe` tests in
   ursula-raft.

## Reaching the target

| Stage                              | Cumulative cut |
| ---------------------------------- | -------------- |
| 1 — mechanical                     | ~8-10k         |
| 2 — structural unification         | ~13-15k        |
| 3 — feature retirement (all items) | ~16-18k        |
| 4 — test-surface pruning           | ~21-24k ✓      |

Stage 1 alone gets the codebase to roughly 85% of its current size with zero
risk. The three-quarters target requires stages 2 and 3 plus at least the
DST-overlap deletion from stage 4. Stages are ordered by risk, not by size:
nothing in stage 2+ starts until stage 1 is merged and green, because the
mechanical shrink makes every later diff smaller and easier to review.
