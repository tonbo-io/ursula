# Ursula event-time index

`ursula-indexer` is a rebuildable materialized index for the client-supplied event time in Ursula JSON record streams. Ursula remains the event source. The indexer consumes record envelopes over HTTP, writes immutable sorted Parquet parts to S3, and conditionally publishes each source-record checkpoint.

The index is deliberately outside Ursula's Raft state machine. S3 is authoritative for the derived index; local disk is only a bounded, disposable Parquet cache. Multiple stateless indexers may race safely because `CURRENT` advances with an ETag compare-and-swap. Losing writers reload the winning checkpoint and verify that overlapping records have identical event times.

```bash
cargo run -p ursula-index --bin ursula-indexer -- \
  --stream-url http://127.0.0.1:4437/v1/stream/browser-telemetry \
  --s3-bucket my-telemetry-index \
  --s3-region us-east-1 \
  --s3-prefix production/browser-telemetry \
  --cache-dir ./target/browser-telemetry-cache
```

Credentials use the standard AWS environment/provider chain. `--s3-endpoint` supports S3-compatible services that implement conditional object creation and ETag-matched writes. For local development, replace the S3 options with `--object-dir ./target/browser-telemetry-objects`; this filesystem backend implements the same immutable-object and conditional-`CURRENT` protocol.

The object layout is `parts/<content-hash>.parquet`, `manifests/<generation>-<content-hash>.json`, and `CURRENT`. Parts and manifests are immutable. Compaction publishes a new manifest but does not synchronously delete objects referenced by an older generation, so readers never observe a half-compacted index. Garbage collection is intentionally a separate lifecycle and is not implemented yet; the production GC must retain objects reachable from `CURRENT` and the configured manifest-history window, protect objects newer than a grace period, then delete the remaining unreferenced parts and manifests, including uploads left by failed CAS attempts.

Queries use the Parquet page index and return stable `(captured_at, record)` cursors together with a fixed `through_record` pagination watermark:

```text
GET /v1/events?from=<RFC3339-or-ms>&until=<RFC3339-or-ms>&limit=1000
GET /v1/status
POST /v1/status/resume
```

The process stops source advancement and publishes a persistent status only for deterministic source-data failures (an invalid timestamp, unexpected record, or conflicting record) or when source retention has passed its rebuild checkpoint. Transient source, S3, cache, and conditional-publication failures are retried without changing the persistent status. An operator can clear `blocked` with the idempotent `POST /v1/status/resume` operation after repairing the source data; a retention gap requires rebuilding the index and cannot be resumed. The default listener is loopback-only, and deployments that expose the query API must restrict this operator endpoint at their HTTP ingress.

Run the opt-in real-S3 recovery test with `URSULA_EVENT_INDEX_S3_INTEGRATION=1`, `URSULA_EVENT_INDEX_S3_BUCKET`, and the optional `URSULA_EVENT_INDEX_S3_REGION` / `URSULA_EVENT_INDEX_S3_ENDPOINT` variables. The test uses and removes a unique prefix.
