# Ursula event-time index

`ursula-indexer` is a rebuildable materialized index for the client-supplied event time in Ursula JSON record streams. Ursula remains the event source. The indexer consumes record envelopes over HTTP, writes immutable sorted Parquet parts to S3, and conditionally publishes source-record progress.

The index is deliberately outside Ursula's Raft state machine. S3 is authoritative for the derived index; local disk is only a bounded, disposable Parquet cache. Multiple stateless workers claim different `(stream, record range)` tasks. Claims reduce duplicate work but are not locks: immutable content-addressed parts and an ETag compare-and-swap on each stream's `CURRENT` manifest provide correctness. Ranges may complete out of order, while `durable_through_record` advances only through a gap-free prefix.

Without `--stream-url`, the binary runs as a dynamic worker pool. Register or remove sources at runtime; adding a stream does not restart pods or change Helm values:

```bash
curl -X PUT http://127.0.0.1:4493/v1/indexes/browser-session-42 \
  -H 'Content-Type: application/json' \
  -d '{"stream_url":"http://ursula:4437/sessions/browser-session-42","timestamp_field":"captured_at"}'
```

The shared S3 root stores the registration catalog and derives a separate logical namespace for every registered source. A fixed worker pool schedules ranges across those namespaces, allowing small streams to share workers and one hot stream to use several workers. Serving and maintenance cache budgets are shared across all registrations in each process rather than multiplied by stream count. Query routes are `/v1/indexes/{id}/events`; registration, deletion, and status resume routes are administrative and should remain behind authenticated internal networking.

The verified-range manifest is format version 5. Older indexes are not adopted automatically; register the source in a new pool root and rebuild from Ursula. This is an intentional prototype-phase format break.

Passing `--stream-url` selects the legacy single-source mode for local development and focused recovery work:

```bash
cargo run -p ursula-index --bin ursula-indexer -- \
  --stream-url http://127.0.0.1:4437/telemetry/browser-telemetry \
  --s3-bucket my-telemetry-index \
  --s3-region us-east-1 \
  --s3-prefix production/browser-telemetry \
  --cache-dir ./target/browser-telemetry-cache \
  --maintenance-cache-max-bytes 536870912 \
  --compact-parts 8 \
  --compaction-max-entries 1000000
```

Credentials use the standard AWS environment/provider chain. `--s3-endpoint` supports S3-compatible services that implement conditional object creation and ETag-matched writes. For local development, replace the S3 options with `--object-dir ./target/browser-telemetry-objects`; this filesystem backend implements the same immutable-object and conditional-`CURRENT` protocol.

The object layout is `parts/<content-hash>.parquet`, `layouts/<content-hash>.json`, `manifests/<generation>-<content-hash>.json`, and `CURRENT`. Every generated part must contain a Parquet offset index; a missing offset index or page location is a format error rather than a signal to fall back to whole column chunks. A layout partitions the file into its native data pages, dictionary prefixes, and header/index/footer gaps, with a BLAKE3 hash for every unit. Parquet's async reader still chooses page and column-chunk byte ranges; the read-through cache expands each request only to the covering verified units, validates bytes before admission and again on every hit, and never invents a second logical row/block format. Foyer deduplicates concurrent misses and manages memory plus local-disk caching. `--cache-max-bytes` remains the total serving-cache local-disk bound and must be at least 16 MiB: three quarters are reserved for verified ranges and one quarter for whole parts needed by conflict verification; Foyer's memory tier is additionally bounded to one eighth of its disk allocation, capped at 128 MiB. The maintenance instance never serves range queries, so all of `--maintenance-cache-max-bytes` is available for whole parts used by compaction. Flushes split entries into UTC-day event-time partitions. Within each partition, up to `--compact-parts` level-0 parts are merged once into an immutable level-1 part; level-1 history is never rewritten, and late events create new level-0 parts in their original day. `--compaction-max-entries` is a hard bound on each merge, so compaction memory and write work do not grow with total index history. If the configured fan-in would exceed the bound, the planner selects a smaller merge and continues scanning later partitions instead of blocking behind one oversized partition.

Garbage collection runs every `--gc-interval-seconds` and retains objects reachable from `CURRENT` plus `--gc-retain-generations` recent compatible manifest generations. Objects with a missing modification time and objects newer than `--gc-grace-seconds` are protected so ambiguous metadata or an in-flight competing indexer cannot cause deletion. Incompatible manifests from an older format or source are warned, skipped, and reclaimed after grace rather than blocking every GC pass. Older unreferenced parts, verified-range layouts, and manifests, including failed-CAS outputs and superseded compaction inputs, are deleted. Expired claims left by crashed workers and claims already covered by the durable watermark are also removed. GC reloads `CURRENT` immediately before deletion and protects that manifest too, so ordinary concurrent publication does not starve reclamation; the grace period must exceed the maximum allowed index publication attempt duration.

Source ingestion and HTTP queries share the serving index and cache. Compaction and GC run on a second serverless index instance with a separate bounded maintenance cache, so Parquet rewrite, S3 upload, full-prefix listing, and retained-manifest reads never hold the query mutex.

Single-source mode exposes:

```text
GET /v1/events?from=<RFC3339-or-ms>&until=<RFC3339-or-ms>&limit=1000
GET /v1/status
POST /v1/status/resume
```

Pool mode exposes the equivalent operations below `/v1/indexes/{id}`. Registration records the source's current `Stream-Record-First` as `indexed_from_record`, so an existing retained stream is indexed from its oldest still-readable record instead of requiring record 0. Status and query responses expose that base, and query responses also return the `indexed-from-record` header. Processing stops for one registration only on deterministic source-data failures or when source retention advances past the registration's unfinished checkpoint. Transient source, S3, cache, lease, and conditional-publication failures are retried without changing persistent status. An operator can clear `blocked` with the idempotent status resume operation after repairing source data; a later retention gap cannot be resumed because the missing registered range is no longer readable.

The pool catalog is one conditionally updated `CATALOG` object rather than one object per registration. A scheduler refresh therefore costs one object read per pod regardless of registration count, and registration or deletion cannot leave a half-updated source-lock pair. Maintenance reconciles its in-memory indexes against this catalog on every pass, so unregistering on one pod stops maintenance and releases the stale instance on every other pod. An unreadable catalog fails readiness and pauses scheduling and reconciliation; it is never treated as an empty catalog because that would turn corruption into an apparent mass unregister.

Run the opt-in real-S3 recovery test with `URSULA_EVENT_INDEX_S3_INTEGRATION=1`, `URSULA_EVENT_INDEX_S3_BUCKET`, and the optional `URSULA_EVENT_INDEX_S3_REGION` / `URSULA_EVENT_INDEX_S3_ENDPOINT` variables. The test uses and removes a unique prefix.
