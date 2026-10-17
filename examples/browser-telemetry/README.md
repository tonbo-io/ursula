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

Consumers read the stream by offset. Start at `offset=-1` for the oldest retained event, then pass back each response's `Stream-Next-Offset` to continue. `Stream-Up-To-Date: true` means the read reached the tail. Offsets are opaque: store the values Ursula returns rather than computing them. Ursula does not reorder events by `captured_at` and has no event-time read. Filter on `captured_at` in the consumer, or keep your own index that maps event times to offsets.

```bash
curl -i 'http://127.0.0.1:4437/telemetry/events?offset=-1'
```

This example does not require an Ursula SDK or Append Session. Production collectors should add durable local retry storage and an authenticated same-origin gateway appropriate to their environment.
