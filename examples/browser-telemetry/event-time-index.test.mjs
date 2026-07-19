import assert from "node:assert/strict";
import test from "node:test";

import { EventTimeIndex, SourceRetentionGap, syncOnce } from "./event-time-index.mjs";

test("out-of-order event time is indexed without changing record order", () => {
  const index = new EventTimeIndex();
  index.ingest({
    record: 0,
    value: { captured_at: "2026-07-18T10:00:02.000Z" },
  });
  index.ingest({
    record: 1,
    value: { captured_at: "2026-07-18T10:00:01.000Z" },
  });
  index.ingest({
    record: 2,
    value: { captured_at: "2026-07-18T10:00:01.000Z" },
  });

  assert.deepEqual(
    index.query({
      from: "2026-07-18T10:00:00.000Z",
      until: "2026-07-18T10:00:03.000Z",
    }),
    { indexed_through_record: 3, records: [1, 2, 0] },
  );
});

test("sync reports a retention gap instead of returning an incomplete answer", async () => {
  const response = new Response(null, {
    status: 410,
    headers: { "Stream-Record-First": "9", "Stream-Record-Next": "12" },
  });
  await assert.rejects(
    syncOnce(new EventTimeIndex(4), "https://example.test/telemetry", async () => response),
    SourceRetentionGap,
  );
});
