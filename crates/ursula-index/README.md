# Ursula event-time index (experimental)

The `ursula indexer` role is a rebuildable index of the event time carried inside the messages of Ursula `application/json` and `application/x-ndjson` streams. Ursula remains the source of truth. The indexer reads streams with ordinary offset reads (`consistency=leader`, so a read never trails the HEAD that detected a recreate), writes immutable sorted Parquet parts to S3, and conditionally publishes how far each stream is indexed. A query returns `(offset, len)` locators that a client resolves with base-protocol reads.

**Experimental.** The HTTP API and the S3 format may change in a minor release. A format change needs a new or emptied S3 prefix and re-registration; the index is rebuilt from the retained source history.

Internal contract: the indexer ships in the same binary as Ursula and relies on Ursula offsets being byte positions, so a message's offset is the read's start offset plus the bytes before it. It returns offsets to clients only as the opaque 20-digit tokens Ursula itself mints; clients never do offset arithmetic.

## Registering a stream

Without `--stream-url`, the binary runs as a dynamic worker pool. Register or remove sources at runtime; adding a stream does not restart pods or change Helm values:

```bash
curl -X PUT http://127.0.0.1:4493/v1/indexes/traces-checkout \
  -H 'Content-Type: application/json' \
  -d '{"stream_url":"http://ursula:4437/otel/traces-checkout-20261003",
       "start":"retained",
       "extract":{"each":"/resourceSpans/*/scopeSpans/*/spans/*",
                  "time":["/startTimeUnixNano"],"end":["/endTimeUnixNano"],"unit":"ns"}}'
```

The extractor is `{each?, time:[pointer…], end?:[pointer…], unit}`:

- Pointers are RFC 6901 JSON pointers in which a `*` segment matches every member of an object or element of an array.
- `each` selects the elements of a message; by default the message itself is the one element.
- `time` and `end` are fallback lists evaluated per element: the first pointer that yields a value wins.
- `unit` is `auto` (an RFC 3339 string or integer milliseconds), `rfc3339`, `s`, `ms`, `us` or `ns`. Values may be numbers or decimal strings, which is how OTLP encodes 64-bit integers; `s` also accepts fractional seconds. `0`, `null` and a missing value count as missing.
- Each message yields one entry: `t_ms` is the minimum `time` across its elements and `t_end_ms` the maximum `end` (or `time`).
- The legacy `{"stream_url":…,"timestamp_field":"captured_at"}` form still works; it means `{"time":["/captured_at"],"unit":"auto"}`. Without either, the extractor reads `captured_at`.

Common extractors:

| Data | Extractor |
| --- | --- |
| OTLP-JSON traces | `{"each":"/resourceSpans/*/scopeSpans/*/spans/*","time":["/startTimeUnixNano"],"end":["/endTimeUnixNano"],"unit":"ns"}` |
| OTLP-JSON logs | `{"each":"/resourceLogs/*/scopeLogs/*/logRecords/*","time":["/timeUnixNano","/observedTimeUnixNano"],"unit":"ns"}` |
| Claude Code transcripts | `{"time":["/timestamp"]}` |
| A capture envelope | `{"time":["/entry/timestamp"]}` |

`start` is `retained` (the default: index all history still readable) or `tail` (only what is appended from now on). Registration records the source's `Stream-Incarnation`. `start` applies to the first registration only: a restart after a recreate covers the new stream from its first byte, since all of it was appended after the registration; bytes retention trimmed before the restart are counted as trimmed.

## What is indexed

Messages are framed by LF in both content types. A message longer than one read is assembled across reads up to 32 MiB; a longer one is read through and counted as `oversize`; a pass that ends inside such a line remembers how far it read and the next pass continues from there. An unterminated last line is not indexed until its LF arrives. Empty or whitespace-only lines are not messages; they are skipped without being counted.

Bad data never blocks a stream. A message without a time is counted as `missing`, one whose time has the wrong type or format as `invalid`, and one that is not JSON as `unparseable`. A stream is blocked only when two workers produced different entries for the same source bytes; the committed prefix stays queryable and `POST /v1/indexes/{id}/status/resume` clears the block after repair.

Retention is followed, not fatal. The index keeps a floor at the source's retained offset and does not return entries below it. Bytes trimmed before they were indexed are counted, and queries then report `complete: false`. When a stream is trimmed in the middle of a message, the indexer discards the rest of that line and counts it as trimmed; a first line that parses as a complete JSON value is kept.

A deleted and recreated stream has a new `Stream-Incarnation`. The pool then retires the registration's namespace and restarts the registration under a new one, `{id}-{url hash}-{incarnation}-{extractor digest}`; `/status` reports `restarted_from_incarnation`. Single-source mode restarts its index in place and does not report `restarted_from_incarnation`. A stream that answers 404 is reported as `source_gone` until it answers again.

## Querying

```text
GET /v1/indexes/{id}/events?from=<RFC3339-or-ms>&until=<RFC3339-or-ms>[&match=start|overlap][&limit=1000][&after=<next>][&through=<coverage.through>]
GET /v1/indexes/{id}/status
POST /v1/indexes/{id}/status/resume
```

```json
{"source":{"stream_url":"http://ursula:4437/otel/traces-checkout-20261003","incarnation":"1759482000123"},
 "coverage":{"from":"00000000000000000000","floor":"00000000000004194304",
             "through":"00000000000187650112","durable":"00000000000187650112",
             "complete":false,"trimmed_bytes":4194304},
 "skipped":{"missing":12,"invalid":0,"unparseable":0,"oversize":0},
 "entries":[{"t_ms":1759482001000,"t_end_ms":1759482004000,
             "offset":"00000000000187600000","len":48211}],
 "next":"80000199a94cb668000000000b2e8c80"}
```

Entries sort by `t_ms`, then by offset. `match=start` (the default) returns events that start inside `[from, until)`; `match=overlap` returns events whose `[t_ms, t_end_ms]` span intersects it. Pass `next` as `after` and `coverage.through` as `through` to page through one fixed watermark. Responses also carry `indexed-from-offset`, `floor-offset`, `durable-offset` and `through-offset` headers.

Fetch one message with `GET {stream_url}?offset=<offset>&max_bytes=<len>`. A read returns at most 8 MiB, so keep reading from each response's `Stream-Next-Offset` until `len` bytes have arrived. Locators are valid for `source.incarnation`; if the stream's `Stream-Incarnation` (HEAD) differs, the stream was recreated and the locators are stale.

## Storage and work distribution

The index is deliberately outside Ursula's Raft state machine. S3 is authoritative for the derived index; local disk is only a bounded, disposable Parquet cache. Each stream has one claim object at a time, which starts at the first unindexed byte and stays open until its holder commits. A claim is an expiring efficiency hint, not a lock: a worker whose claim expired may still commit, and overlapping commits must produce exactly the same entries for the same bytes. Immutable content-addressed parts and an ETag compare-and-swap on each stream's `CURRENT` manifest provide correctness. A worker reads until `--segment-bytes` or `--flush-entries`, or until the tail. The rest after a `--flush-entries` stop is claimed right away; any other segment shorter than `--segment-bytes` is committed at most once per `--tail-flush-interval-ms`.

The shared S3 root stores the registration catalog and one namespace per registration. A fixed worker pool schedules streams across namespaces, allowing small streams to share workers. Serving and maintenance cache budgets are shared across all registrations in each process rather than multiplied by stream count. Registration, deletion, and resume are administrative routes and should remain behind authenticated internal networking.

The manifest is format version 6 and the catalog version 2. Older indexes are not adopted: use a new or emptied prefix and register again.

Passing `--stream-url` selects single-source mode for local development and focused recovery work. It takes `--extract '<json>'` or `--timestamp-field <name>`, and `--start retained|tail`; on a recreated source it restarts the index in place. Run one single-source process per index prefix: unlike the pool, it does not guard a segment read from the old stream against a restart by another process.

```bash
cargo run -p ursula --bin ursula -- indexer \
  --stream-url http://127.0.0.1:4437/telemetry/browser-telemetry \
  --timestamp-field captured_at \
  --s3-bucket my-telemetry-index \
  --s3-region us-east-1 \
  --s3-prefix production/browser-telemetry \
  --cache-dir ./target/browser-telemetry-cache
```

Single-source mode exposes `GET /v1/events`, `GET /v1/status` and `POST /v1/status/resume`.

Credentials use the standard AWS environment/provider chain. `--s3-endpoint` supports S3-compatible services that implement conditional object creation and ETag-matched writes. For local development, replace the S3 options with `--object-dir ./target/browser-telemetry-objects`; this filesystem backend implements the same immutable-object and conditional-`CURRENT` protocol.

The object layout is `parts/<content-hash>.parquet`, `layouts/<content-hash>.json`, `manifests/<generation>-<content-hash>.json`, `claims/current.json` and `CURRENT`. Parts have the columns `t_ms`, `t_end_ms`, `offset` and `len`, read by name so later columns are additive. Every generated part must contain a Parquet offset index; a missing offset index or page location is a format error rather than a signal to fall back to whole column chunks. A layout partitions the file into its native data pages, dictionary prefixes, and header/index/footer gaps, with a BLAKE3 hash for every unit. Parquet's async reader still chooses page and column-chunk byte ranges; the read-through cache expands each request only to the covering verified units, validates bytes before admission and again on every hit, and never invents a second logical row/block format. Foyer deduplicates concurrent misses and manages memory plus local-disk caching. `--cache-max-bytes` remains the total serving-cache local-disk bound and must be at least 16 MiB: three quarters are reserved for verified ranges and one quarter for whole parts needed by overlap verification; Foyer's memory tier is additionally bounded to one eighth of its disk allocation, capped at 128 MiB. The maintenance instance never serves range queries, so all of `--maintenance-cache-max-bytes` is available for whole parts used by compaction.

Commits split entries into UTC-day event-time partitions. Within each partition, same-level parts are merged with `--compact-parts` fan-in into the next immutable level; late events create new level-0 parts in their original day and join the same bounded compaction tree. Compaction also drops entries below the floor. `--compaction-max-entries` is a hard bound on each merge, so compaction memory and write work do not grow with total index history. If the configured fan-in would exceed the bound, the planner selects a smaller merge and continues scanning later tiers instead of blocking behind one oversized tier.

Garbage collection runs every `--gc-interval-seconds` and retains objects reachable from `CURRENT` plus `--gc-retain-generations` recent compatible manifest generations. Objects with a missing modification time and objects newer than `--gc-grace-seconds` are protected so ambiguous metadata or an in-flight competing indexer cannot cause deletion. Incompatible manifests from an older format or source are warned, skipped, and reclaimed after grace rather than blocking every GC pass. Expired claims left by crashed workers are removed. Unregistering an index, or restarting it for a recreated source, records a tombstone for its namespace so every pool replica stops writing it; after the same grace period, maintenance deletes the namespace.

Source ingestion and HTTP queries share the serving index and cache. Compaction and GC run on a second index instance with a separate bounded maintenance cache, so Parquet rewrite, S3 upload, full-prefix listing, and retained-manifest reads never hold the query mutex.

The pool catalog is one conditionally updated `CATALOG` object rather than one object per registration. A scheduler refresh therefore costs one object read per pod regardless of registration count. Maintenance reconciles its in-memory indexes against this catalog on every pass. An unreadable catalog fails readiness and pauses scheduling and reconciliation; it is never treated as an empty catalog because that would turn corruption into an apparent mass unregister.

Run the opt-in real-S3 recovery test with `URSULA_EVENT_INDEX_S3_INTEGRATION=1`, `URSULA_EVENT_INDEX_S3_BUCKET`, and the optional `URSULA_EVENT_INDEX_S3_REGION` / `URSULA_EVENT_INDEX_S3_ENDPOINT` variables. The test uses and removes a unique prefix, under `URSULA_S3_PREFIX` when that is set (CI runs it on AWS S3 that way). The end-to-end test against an in-process Ursula is `crates/ursula/tests/event_index_e2e.rs`.
