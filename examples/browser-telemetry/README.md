# Browser telemetry

This example sends browser events to an `application/json` Ursula stream with plain `fetch`. Every event carries its one authoritative client timestamp in `captured_at`; Ursula stores each event as one message in commit order.

Serve the browser app behind a same-origin HTTP reverse proxy that maps `/telemetry/events` to the Ursula stream. The gateway owns authentication and CORS policy; Ursula remains the Durable Streams origin.

```js
import {
  createTelemetryCollector,
  installBrowserTelemetry,
} from "./collector.js";

const telemetry = createTelemetryCollector({ streamUrl: "/telemetry/events" });
installBrowserTelemetry(telemetry);
telemetry.capture("application_started", { release: "2026.07.18" });
```

`ursula indexer` (experimental) is the persistent implementation of the external derived-index contract from issue #86. It reads the stream with ordinary offset reads, writes immutable Parquet parts sorted by `(captured_at, offset)` to S3, conditionally publishes how far the stream is indexed, and returns `(offset, len)` locators. Events without a usable `captured_at` are skipped and counted; retention that trims events before they were indexed is reported as `"complete": false`. S3 is authoritative; the local directory is only a bounded cache and can disappear between invocations.

```bash
cargo run -p ursula --bin ursula -- indexer \
  --stream-url http://127.0.0.1:4437/telemetry/browser-telemetry \
  --timestamp-field captured_at \
  --s3-bucket my-telemetry-index \
  --s3-prefix production/browser-telemetry \
  --cache-dir ./target/browser-telemetry-cache
```

For a local run, use `--object-dir ./target/browser-telemetry-objects` instead of the S3 options. This exercises the same immutable manifest and conditional `CURRENT` design; it is not the production durability boundary.

Query event time over ordinary HTTP. A query returns entries with an opaque `offset` and a `len`; fetch an event with `GET {stream}?offset=<offset>&max_bytes=<len>`, continuing from `Stream-Next-Offset` until `len` bytes have arrived. Subsequent pages pass the previous response's `next` as `after` and its `coverage.through` as `through`.

```bash
curl 'http://127.0.0.1:4493/v1/events?from=2026-07-18T10%3A00%3A00Z&until=2026-07-18T11%3A00%3A00Z&limit=100'
curl 'http://127.0.0.1:4493/v1/status'
```

This example does not require an Ursula SDK or Append Session. Production collectors should add durable local retry storage and an authenticated same-origin gateway appropriate to their environment.
