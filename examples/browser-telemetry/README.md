# Browser telemetry

This example sends browser events to an `application/json` Ursula stream with plain `fetch`. Every event carries its one authoritative client timestamp in `captured_at`; Ursula assigns stable record ordinals in commit order.

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

`ursula-indexer` is the persistent implementation of the external derived-index contract from issue #86. It consumes the envelope view in record order, writes immutable Parquet parts sorted by `(captured_at, record)` to S3, conditionally publishes each source checkpoint, and reports a retention gap instead of returning incomplete history. S3 is authoritative; the local directory is only a bounded cache and can disappear between invocations.

```bash
cargo run -p ursula-index --bin ursula-indexer -- \
  --stream-url http://127.0.0.1:4437/v1/stream/browser-telemetry \
  --s3-bucket my-telemetry-index \
  --s3-prefix production/browser-telemetry \
  --cache-dir ./target/browser-telemetry-cache
```

For a local run, use `--object-dir ./target/browser-telemetry-objects` instead of the S3 options. This exercises the same immutable manifest and conditional `CURRENT` design; it is not the production durability boundary.

Query event time over ordinary HTTP. `through_record` pins pagination to one fully indexed source prefix; subsequent pages pass `after_captured_at_ms` and `after_record` from the previous response's `next` cursor.

```bash
curl 'http://127.0.0.1:4493/v1/events?from=2026-07-18T10%3A00%3A00Z&until=2026-07-18T11%3A00%3A00Z&limit=100'
curl 'http://127.0.0.1:4493/v1/status'
```

This example does not require an Ursula SDK or Append Session. Production collectors should add durable local retry storage and an authenticated same-origin gateway appropriate to their environment.
