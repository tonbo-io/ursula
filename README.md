# Ursula

[![Crates.io](https://img.shields.io/crates/v/ursula.svg)](https://crates.io/crates/ursula)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Docs: **[ursula.tonbo.io](https://ursula.tonbo.io)**

Ursula is a self-hosted, distributed server for the replayable, append-only event timelines behind document edits, agent runs, workflows, and chat. It speaks the [Durable Streams Protocol](https://github.com/durable-streams/durable-streams) over plain HTTP and SSE.

## What Ursula keeps

Event streams live outside the broker network. Document editors, agents, and durable workflows need timelines that browsers, mobile apps, and serverless functions can read, write, and tail over the public internet. That asks for HTTP-native, distributed, S3-backed infrastructure, not the SDK-locked, single-network shape Kafka-style brokers were built for.

The [Durable Streams Protocol](https://github.com/durable-streams/durable-streams) nails that wire format, but its reference server is a single process: a node loss is data loss. The other servers we evaluated each force you to give up one of four things this primitive deserves to keep:

- **Open-source self-hosting.**
- **Low write latency** (sub-50 ms P99 appends, no batching window required).
- **Plain S3 economics** (cold tier on standard S3, no S3 Express tier, no per-GB SaaS markup).
- **Quorum-replicated durability** (acknowledged writes survive a single-node failure).

Ursula keeps all four.

Full design intent: [Why Ursula](https://ursula.tonbo.io/docs/why-ursula) · [How Ursula compares](https://ursula.tonbo.io/docs/competitive-comparison).

## Quickstart

> For now, Ursula builds from Rust source. Pre-built release binaries are on the way.

Run a single in-memory node (no persistence, good for kicking the tires):

```bash
cargo run --bin ursula -- --preset default
```

It binds `127.0.0.1:4437`, picks a core count from your CPU, and uses an in-memory engine. Use `--config` to load a TOML config file, or `--preset` to choose a built-in resource preset.

Create a bucket and stream, append bytes, read them back:

```bash
curl -X PUT http://127.0.0.1:4437/demo
curl -X PUT http://127.0.0.1:4437/demo/hello

curl -X POST http://127.0.0.1:4437/demo/hello \
  -H 'Content-Type: application/octet-stream' \
  --data-binary 'hello world'

curl 'http://127.0.0.1:4437/demo/hello?offset=-1'
```

Tail the stream live over SSE, new appends arrive as `event: data` lines immediately:

```bash
curl -N 'http://127.0.0.1:4437/demo/hello?offset=-1&live=sse'
```

Walkthroughs: [Quick Start](https://ursula.tonbo.io/docs/quick-start) · [Deploy a cluster](https://ursula.tonbo.io/docs/deploy-cluster) · [Configure S3](https://ursula.tonbo.io/docs/configure-s3).

## Architecture

Three or five Ursula processes act as one durable-streams server. A stream hashes to one Raft group, that group has one replica on each voter node, and the same group ID is owned by a deterministic core on every node. Groups replicate independently; there is no cross-group transaction path.

```text
                  HTTP / SSE clients
        |                 |                 |
        v                 v                 v
        route(bucket_id, stream_id)
        |
        v
  +-----------+     +-----------+     +-----------+
  |  node 1   |<--->|  node 2   |<--->|  node 3   |
  | HTTP/gRPC |     | HTTP/gRPC |     | HTTP/gRPC |
  |           |     |           |     |           |
  | core 0    |     | core 0    |     | core 0    |
  |  group 0* |<--->|  group 0  |<--->|  group 0  |
  |  group 3  |<--->|  group 3* |<--->|  group 3  |
  |           |     |           |     |           |
  | core 1    |     | core 1    |     | core 1    |
  |  group 1  |<--->|  group 1* |<--->|  group 1  |
  |  group 4* |<--->|  group 4  |<--->|  group 4  |
  |           |     |           |     |           |
  | core 2    |     | core 2    |     | core 2    |
  |  group 2  |<--->|  group 2  |<--->|  group 2* |
  |  group 5  |<--->|  group 5  |<--->|  group 5* |
  +-----+-----+     +-----+-----+     +-----+-----+
        |                 |                 |
        +-----------------+-----------------+
                          |  background flush
                          v
                   +--------------+
                   | S3 cold tier |
                   +--------------+

  * leader for that Raft group, leadership can differ per group.
```

- **[Thread-per-core](https://seastar.io/shared-nothing/), [multi-Raft](https://tikv.org/deep-dive/scalability/multi-raft/).**

  Each stream hashes to one Raft group and owner core, so cores own disjoint groups with no shared mutable state on the hot path.

- **Per-group node-to-node Raft.**

  Every node hosts replicas for the same configured groups, and those replicas exchange gRPC Raft RPCs while non-leader HTTP writes receive a `307` redirect to the current group leader.

- **Hot ring on the write path.**

  Appends commit into an in-memory ring and Raft log while background flushers move older committed chunks to S3.

- **Independent Raft groups.**

  Each group has its own raft instance, log, state machine, hot ring, watchers, and cold-flush budget, with no cross-group commit protocol.

- **Stateless HTTP front door.**

  [axum](https://github.com/tokio-rs/axum) parses, routes, and renders the protocol while stream ownership and mutable state stay inside the owning group actor.

Across nodes, writes are leader-serialized within one group and acknowledged after a majority of that group's replicas persist and apply the command. Full design: [Architecture overview](https://ursula.tonbo.io/docs/architecture/overview).

## Benchmark

On EC2 (3 × `c7g.4xlarge`, Raft quorum), Ursula sustains **35.2k appends/sec** at 500 streams (5.9× single-node Durable Streams, 5.2× S2 Lite, both on 1 × `c7g.4xlarge`) and delivers SSE fan-out to 1000 subscribers at **6.1 ms p99** (160× faster than Durable Streams, 18× faster than S2 Lite). Apples-to-apples methodology, full charts, replay and latency cuts: [ursula.tonbo.io/benchmark](https://ursula.tonbo.io/benchmark).

## Roadmap

The `v0.1.x` line is a working prototype. Next on deck:

- [ ] **`if-match` conditional append.**

  Optimistic concurrency control on the append path. An `if-match: <offset>` header lets a writer commit only when the stream tip hasn't moved, so concurrent writers can coordinate without an external lock. The semantics need to land in Ursula's HTTP adapter and Raft state machine.

- [ ] **Stateless WASM compute over streams.**

  A planned Ursula extension: bind a deterministic WASM module to a stream so the server can materialize per-stream state, enabling automatic compaction and `410 Gone` bootstrap recovery without application-side checkpointing.

- [ ] **Dynamic membership.**

  Online voter / learner reconfiguration and orchestrated rolling membership changes (today's clusters are static).

- [ ] **Backup and restore tooling.**

  A supported recovery path for total-cluster loss from the S3 cold tier (today there is none).

- [ ] **Client SDKs.**

  Ergonomic Rust and TypeScript clients on top of the HTTP API.

## Credits

- **[ElectricSQL](https://electric-sql.com/)** for the original Durable Streams Protocol that Ursula implements.
- **[Loro](https://loro.dev/)** for the snapshot and replay extension design that Ursula adopted on top of the base protocol.

## License

Apache 2.0. See [LICENSE](LICENSE).

Built by [Tonbo](https://tonbo.io/), an open-source storage team.
