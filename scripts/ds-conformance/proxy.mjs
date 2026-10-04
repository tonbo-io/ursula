// Path adapter between the official Durable Streams conformance suite and Ursula.
//
// The suite addresses streams as `/v1/stream/<name>`. Ursula serves streams as
// `/{bucket}/{stream}`, and bucket IDs must be 4-64 bytes, so `/v1/...` is not
// a valid Ursula path. This proxy rewrites `/v1/stream/<name>` to
// `/<bucket>/<name>` (any other `/v1/<rest>` to `/<bucket>/<rest>`) and
// rewrites `Location` headers back to `/v1/stream/<name>`. Bodies stream
// through unbuffered, so SSE and long-poll work.
//
// Usage: node proxy.mjs <listen-port> <upstream-base-url> [bucket]
import http from "node:http";

const [listenPort, upstreamBase, bucket = "conformance"] = process.argv.slice(2);
if (!listenPort || !upstreamBase) {
  console.error("usage: node proxy.mjs <listen-port> <upstream-base-url> [bucket]");
  process.exit(2);
}
const upstream = new URL(upstreamBase);

const SUITE_STREAM_PREFIX = "/v1/stream/";

function toUpstream(path) {
  if (path.startsWith(SUITE_STREAM_PREFIX)) {
    return `/${bucket}/${path.slice(SUITE_STREAM_PREFIX.length)}`;
  }
  return path.startsWith("/v1/") ? `/${bucket}/${path.slice(4)}` : path;
}

function fromUpstream(location) {
  try {
    const url = new URL(location, upstream);
    if (url.pathname.startsWith(`/${bucket}/`)) {
      return `${SUITE_STREAM_PREFIX}${url.pathname.slice(bucket.length + 2)}${url.search}`;
    }
  } catch {
    // Leave an unparseable Location untouched.
  }
  return location;
}

const agent = new http.Agent({ keepAlive: true, maxSockets: 256 });

const server = http.createServer((req, res) => {
  const headers = { ...req.headers, host: upstream.host };
  const forward = http.request(
    {
      host: upstream.hostname,
      port: upstream.port,
      method: req.method,
      path: toUpstream(req.url ?? "/"),
      headers,
      agent,
    },
    (upstreamRes) => {
      const out = { ...upstreamRes.headers };
      if (out.location) out.location = fromUpstream(out.location);
      res.writeHead(upstreamRes.statusCode ?? 502, out);
      upstreamRes.pipe(res);
    },
  );
  forward.on("error", (err) => {
    if (!res.headersSent) res.writeHead(502, { "content-type": "text/plain" });
    res.end(`proxy error: ${err.message}`);
  });
  req.on("aborted", () => forward.destroy());
  res.on("close", () => forward.destroy());
  req.pipe(forward);
});

server.keepAliveTimeout = 60_000;
server.listen(Number(listenPort), "127.0.0.1", () => {
  console.log(`conformance proxy :${listenPort} -> ${upstream.origin}/${bucket}`);
});
