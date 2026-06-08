# Ursula Gateway

`ursulagw` is a small HTTP/SSE gateway for Ursula clusters. It gives public
clients one stable HTTP endpoint while keeping internal Ursula node addresses
out of public responses.

The gateway is intentionally thin: it does not parse the Durable Streams
Protocol, own stream routing state, buffer request bodies, terminate SSE
streams, or follow redirects internally.

## Behavior

- Picks one configured upstream Ursula node for each incoming request.
- Streams request and response bodies without buffering them.
- Returns redirects to clients instead of following them internally.
- Rewrites redirect targets so clients continue through the gateway.
- Keeps long-lived SSE reads open after response headers arrive.

## Redirects

The gateway returns Ursula `307 Temporary Redirect` responses to the client.
Redirect-following clients such as `curl -L` continue through the gateway
instead of connecting to internal Ursula nodes directly.

## SSE Behavior

`live=sse` responses stream through the gateway. `--response-header-timeout`
only covers the time needed to receive response headers; streamed response
bodies remain open.

Example:

```bash
curl -N 'http://127.0.0.1:8080/demo/hello?offset=-1&live=sse'
```

## Run

Start a gateway in front of one or more Ursula HTTP or HTTPS nodes:

```bash
cargo run -p ursula-gateway --bin ursulagw -- \
  --listen 127.0.0.1:8080 \
  --upstream http://127.0.0.1:4437 \
  --upstream http://127.0.0.1:4438 \
  --upstream http://127.0.0.1:4439
```

Then send normal Durable Streams requests to the gateway:

```bash
curl -X PUT http://127.0.0.1:8080/demo
curl -X PUT http://127.0.0.1:8080/demo/hello

curl -X POST http://127.0.0.1:8080/demo/hello \
  -H 'Content-Type: application/octet-stream' \
  --data-binary 'hello world'

curl 'http://127.0.0.1:8080/demo/hello?offset=-1'
curl -N 'http://127.0.0.1:8080/demo/hello?offset=-1&live=sse'
```

## Options

```text
--listen <ADDR>
    Address to bind. Defaults to 0.0.0.0:4437.

--upstream <URL>
    Base URL for an Ursula HTTP or HTTPS node. Repeat once per node. Required.

--response-header-timeout <SECONDS>
    Timeout for sending the upstream request and receiving response headers.
    Streamed response bodies such as SSE live reads are not covered.
    Defaults to 30.

--connect-timeout <SECONDS>
    TCP connect timeout per upstream request attempt. Defaults to 5.
```

## Operational Notes

- Upstream URLs support `http` and `https`.
- Upstreams are selected randomly per request.
- Request bodies are streamed; body size limits are enforced by the upstream
  Ursula server, not by the gateway.
- The gateway is stateless. It does not cache Raft leaders or maintain cluster
  membership.
- `RUST_LOG=ursula_gateway=debug` enables request forwarding and redirect logs.

## Verify

```bash
cargo fmt --all -- --check
cargo test -p ursula-gateway
cargo clippy -p ursula-gateway --all-targets -- -D warnings
```
