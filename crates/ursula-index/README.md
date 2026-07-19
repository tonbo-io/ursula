# Ursula event-time index

`ursula-indexer` is a rebuildable materialized index for the client-supplied event time in Ursula JSON record streams. Ursula remains the event source. The indexer consumes record envelopes over HTTP, writes immutable sorted Parquet parts to S3, and conditionally publishes each source-record checkpoint.

The index is deliberately outside Ursula's Raft state machine. S3 is authoritative for the derived index; local disk is only a bounded, disposable Parquet cache. Multiple stateless indexers may race safely because `CURRENT` advances with an ETag compare-and-swap. Losing writers reload the winning checkpoint and verify that overlapping records have identical event times.

```bash
cargo run -p ursula-index --bin ursula-indexer -- \
  --stream-url http://127.0.0.1:4437/v1/stream/browser-telemetry \
  --s3-bucket my-telemetry-index \
  --s3-region us-east-1 \
  --s3-prefix production/browser-telemetry \
  --cache-dir ./target/browser-telemetry-cache \
  --maintenance-cache-max-bytes 536870912 \
  --compact-parts 8 \
  --compaction-max-entries 1000000
```

Credentials use the standard AWS environment/provider chain. `--s3-endpoint` supports S3-compatible services that implement conditional object creation and ETag-matched writes. For local development, replace the S3 options with `--object-dir ./target/browser-telemetry-objects`; this filesystem backend implements the same immutable-object and conditional-`CURRENT` protocol.

The object layout is `parts/<content-hash>.parquet`, `manifests/<generation>-<content-hash>.json`, and `CURRENT`. Flushes split entries into UTC-day event-time partitions. Within each partition, up to `--compact-parts` level-0 parts are merged once into an immutable level-1 part; level-1 history is never rewritten, and late events create new level-0 parts in their original day. `--compaction-max-entries` is a hard bound on each merge, so compaction memory and write work do not grow with total index history. If the configured fan-in would exceed the bound, the planner selects a smaller merge and continues scanning later partitions instead of blocking behind one oversized partition.

Garbage collection runs every `--gc-interval-seconds` and retains objects reachable from `CURRENT` plus `--gc-retain-generations` recent compatible manifest generations. Objects with a missing modification time and objects newer than `--gc-grace-seconds` are protected so ambiguous metadata or an in-flight competing indexer cannot cause deletion. Incompatible manifests from an older format or source are warned, skipped, and reclaimed after grace rather than blocking every GC pass. Older unreferenced parts and manifests, including failed-CAS outputs and superseded compaction inputs, are deleted. GC reloads `CURRENT` immediately before deletion and protects that manifest too, so ordinary concurrent publication does not starve reclamation; the grace period must exceed the maximum allowed index publication attempt duration.

Source ingestion and HTTP queries share the serving index and cache. Compaction and GC run on a second serverless index instance with a separate bounded maintenance cache, so Parquet rewrite, S3 upload, full-prefix listing, and retained-manifest reads never hold the query mutex.

Queries use the Parquet page index and return stable `(captured_at, record)` cursors together with a fixed `through_record` pagination watermark:

```text
GET /v1/events?from=<RFC3339-or-ms>&until=<RFC3339-or-ms>&limit=1000
GET /v1/status
POST /v1/status/resume
```

The process stops source advancement and publishes a persistent status only for deterministic source-data failures (an invalid timestamp, unexpected record, or conflicting record) or when source retention has passed its rebuild checkpoint. Transient source, S3, cache, and conditional-publication failures are retried without changing the persistent status. An operator can clear `blocked` with the idempotent `POST /v1/status/resume` operation after repairing the source data; a retention gap requires rebuilding the index and cannot be resumed. The default listener is loopback-only, and deployments that expose the query API must restrict this operator endpoint at their HTTP ingress.

Run the opt-in real-S3 recovery test with `URSULA_EVENT_INDEX_S3_INTEGRATION=1`, `URSULA_EVENT_INDEX_S3_BUCKET`, and the optional `URSULA_EVENT_INDEX_S3_REGION` / `URSULA_EVENT_INDEX_S3_ENDPOINT` variables. The test uses and removes a unique prefix.
