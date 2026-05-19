# Ursula

[![Crates.io](https://img.shields.io/crates/v/ursula.svg)](https://crates.io/crates/ursula)
[![Documentation](https://docs.rs/ursula/badge.svg)](https://docs.rs/ursula)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

📖 Docs: **[ursula.tonbo.io](https://ursula.tonbo.io)**

## What

Ursula is a thread-per-core, multi-Raft server for the [Durable Streams Protocol](https://github.com/durable-streams/durable-streams): distributed, HTTP-native, append-only byte streams with quorum-replicated writes and optional S3-backed cold storage.

## Why

Every modern app produces a timeline: document edits, agent runs, workflow steps, collaborative strokes. The shape is always the same: ordered, append-only, replayable, live-tailable. There is no shared infrastructure for it. Teams keep rebuilding it on databases, brokers, or object stores, each time with the same recovery edge cases.

The [Durable Streams Protocol](https://github.com/durable-streams/durable-streams) is the right primitive: small, HTTP-native, no required client library. But its reference server runs as a single process, so a node loss is data loss. The alternatives we evaluated each force you to give up one of three things this primitive deserves to keep:

- **Open-source self-hosting.**
- **Low write latency** (sub-50 ms appends without paying S3 Express prices or batching to 250 ms+).
- **Quorum-replicated durability** (acknowledged writes survive a single-node failure).

Ursula is what it looks like to keep all three. Clients see the same URLs, headers, and SSE format the protocol specifies. Three or five nodes underneath act as one durable-streams server, with:

- Per-group Raft replication, leader-serialized appends, transparent follower forwarding.
- An in-memory hot ring on the write path so appends commit in low milliseconds. A background flusher carries chunks to S3 for long-tail durability. S3 is never in the hot path.
- Thread-per-core, multi-Raft placement so aggregate throughput scales with the number of healthy cores across the cluster, not with the bandwidth of a single Raft leader.

Full design intent: [Why Ursula](https://ursula.tonbo.io/docs/why-ursula) · [Architecture overview](https://ursula.tonbo.io/docs/architecture/overview).

## How

Run a single in-memory node (no persistence, good for kicking the tires):

```bash
cargo run -p ursula --bin ursula -- \
  --listen 127.0.0.1:4437 \
  --core-count 4 \
  --raft-group-count 64 \
  --raft-memory
```

Create a bucket and stream, append bytes, read them back:

```bash
curl -X PUT http://127.0.0.1:4437/demo
curl -X PUT http://127.0.0.1:4437/demo/hello

curl -X POST http://127.0.0.1:4437/demo/hello \
  -H 'Content-Type: application/octet-stream' \
  --data-binary 'hello world'

curl 'http://127.0.0.1:4437/demo/hello?offset=-1'
```

Walkthroughs: [Quick Start](https://ursula.tonbo.io/docs/quick-start) · [Deploy a cluster](https://ursula.tonbo.io/docs/deploy-cluster) · [Configure S3](https://ursula.tonbo.io/docs/configure-s3).

## Next steps

The `v0.1.x` line is a working prototype. The roadmap from here:

- **`if-match` conditional append.** Optimistic concurrency control on the append path. An `if-match: <offset>` header lets a writer commit only when the stream tip hasn't moved, so concurrent writers can coordinate without an external lock. Already implemented in `riverrun`, and the same semantics need to land in Ursula's HTTP adapter and Raft state machine.
- **WASM stateless compute extension.** Implement the [Durable Streams WASM compute extension](https://github.com/durable-streams/durable-streams): bind a deterministic WASM module to a stream and let the server materialize per-stream state, enabling automatic compaction and `410 Gone` bootstrap recovery without application-side checkpointing.
- **Dynamic membership.** Online voter / learner reconfiguration and orchestrated rolling membership changes (today's clusters are static).
- **Backup and restore tooling.** A supported recovery path for total-cluster loss from the S3 cold tier (today there is none).
- **Client SDKs.** Ergonomic Rust and TypeScript clients on top of the HTTP API.

Current status across every checklist item: [final goal audit](docs/migration/final-goal-audit.md).

## License

Apache 2.0. See [LICENSE](LICENSE).

Built by [Tonbo](https://tonbo.io/).
