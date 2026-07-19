export class SourceRetentionGap extends Error {}

export class EventTimeIndex {
  constructor(firstRecord = 0) {
    this.indexedThroughRecord = firstRecord;
    this.entries = [];
  }

  ingest(envelope) {
    if (envelope.record !== this.indexedThroughRecord) {
      throw new Error(
        `expected record ${this.indexedThroughRecord}, received ${envelope.record}`,
      );
    }
    const capturedAtMs = Date.parse(envelope.value.captured_at);
    if (!Number.isFinite(capturedAtMs)) throw new Error("invalid captured_at");
    this.entries.push({ capturedAtMs, record: envelope.record });
    this.entries.sort(
      (left, right) =>
        left.capturedAtMs - right.capturedAtMs || left.record - right.record,
    );
    this.indexedThroughRecord += 1;
  }

  query({ from, until }) {
    const fromMs = Date.parse(from);
    const untilMs = Date.parse(until);
    return {
      indexed_through_record: this.indexedThroughRecord,
      records: this.entries
        .filter(
          ({ capturedAtMs }) => capturedAtMs >= fromMs && capturedAtMs < untilMs,
        )
        .map(({ record }) => record),
    };
  }
}

export async function syncOnce(index, streamUrl, fetchImpl = globalThis.fetch) {
  const url = new URL(streamUrl);
  url.searchParams.set("record", String(index.indexedThroughRecord));
  url.searchParams.set("record_view", "envelope");
  const response = await fetchImpl(url);
  if (response.status === 410) {
    const first = Number(response.headers.get("Stream-Record-First"));
    throw new SourceRetentionGap(
      `source retention advanced to record ${first}; rebuild from a covering snapshot`,
    );
  }
  if (!response.ok) throw new Error(`record read failed: ${response.status}`);
  for (const line of (await response.text()).split("\n")) {
    if (line) index.ingest(JSON.parse(line));
  }
  return index.indexedThroughRecord;
}
