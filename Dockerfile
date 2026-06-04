# syntax=docker/dockerfile:1
ARG RUST_VERSION=1.95.0
FROM rust:${RUST_VERSION}-bookworm AS builder

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /ursula
COPY . .

RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
  --mount=type=cache,sharing=locked,target=/usr/local/cargo/git \
  --mount=type=cache,sharing=locked,target=/app/target \
  cargo build --release --locked --bin ursula \
  && strip --strip-debug target/release/ursula \
  && install -Dm755 target/release/ursula /usr/local/bin/ursula

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/ursula /usr/local/bin/ursula

ENV RUST_LOG=info
EXPOSE 4437

ENTRYPOINT ["/usr/local/bin/ursula"]
CMD ["--listen", "0.0.0.0:4437", "--raft-memory"]
