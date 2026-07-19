export function createTelemetryCollector({
  streamUrl,
  fetchImpl = globalThis.fetch,
  flushSize = 20,
}) {
  let queue = [];

  function capture(type, fields = {}) {
    queue.push({
      captured_at: new Date().toISOString(),
      type,
      ...fields,
    });
    if (queue.length >= flushSize) void flush();
  }

  async function flush({ keepalive = false } = {}) {
    if (queue.length === 0) return null;
    const batch = queue;
    queue = [];
    try {
      const response = await fetchImpl(streamUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(batch),
        keepalive,
      });
      if (!response.ok) throw new Error(`telemetry append failed: ${response.status}`);
      return {
        recordStart: Number(response.headers.get("Stream-Record-Start")),
        recordNext: Number(response.headers.get("Stream-Record-Next")),
      };
    } catch (error) {
      queue = batch.concat(queue);
      throw error;
    }
  }

  return { capture, flush, pending: () => queue.length };
}

export function installBrowserTelemetry(collector) {
  const navigation = performance.getEntriesByType("navigation")[0];
  if (navigation) {
    collector.capture("navigation", {
      duration_ms: navigation.duration,
      transfer_bytes: navigation.transferSize,
    });
  }
  addEventListener("error", (event) => {
    collector.capture("window_error", {
      message: event.message,
      source: event.filename,
      line: event.lineno,
    });
  });
  addEventListener("pagehide", () => void collector.flush({ keepalive: true }));
}
