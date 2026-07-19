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

`event-time-index.mjs` demonstrates the external derived-index contract from issue #86. It consumes the envelope view in record order, sorts event-time entries by `(captured_at, record)`, publishes `indexed_through_record`, and fails explicitly if retention makes its answer incomplete.

```bash
node --test examples/browser-telemetry/event-time-index.test.mjs
```

This example does not require an Ursula SDK or Append Session. Production collectors should add durable local retry storage and an authenticated same-origin gateway appropriate to their environment.
