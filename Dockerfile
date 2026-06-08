# syntax=docker/dockerfile:1.7
# RUST_VERSION only selects the base image (cargo/rustup bootstrap). The actual
# build toolchain is pinned by rust-toolchain.toml and installed below.
ARG RUST_VERSION=1.96.0
FROM rust:${RUST_VERSION}-bookworm AS builder

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /ursula

# Install the toolchain pinned in rust-toolchain.toml before copying the full
# source so the layer is cached until the pin changes.
COPY rust-toolchain.toml ./
RUN rustup toolchain install

# Copy dependency manifests first to leverage Docker layer caching for
# dependency downloads.
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# Build with buildx cache mounts for cargo registry, git deps, and build
# artifacts. sharing=locked prevents concurrent writes during parallel builds.
# Because target/ is mounted as a cache and not persisted to the layer, install
# the final binaries into a persistent path within the same RUN so they can be
# extracted in the runtime stage.
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
  --mount=type=cache,sharing=locked,target=/usr/local/cargo/git \
  --mount=type=cache,sharing=locked,target=/ursula/target \
  cargo build --release --locked --bin ursula --bin ursulactl --bin ursulagw \
  && strip --strip-debug target/release/ursula \
  && strip --strip-debug target/release/ursulactl \
  && strip --strip-debug target/release/ursulagw \
  && install -Dm755 target/release/ursula /usr/local/bin/ursula \
  && install -Dm755 target/release/ursulactl /usr/local/bin/ursulactl \
  && install -Dm755 target/release/ursulagw /usr/local/bin/ursulagw

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && groupadd --gid 10001 ursula \
  && useradd \
  --uid 10001 \
  --gid 10001 \
  --home-dir /var/lib/ursula \
  --no-create-home \
  --shell /usr/sbin/nologin \
  ursula \
  && mkdir -p /var/lib/ursula /etc/ursula \
  && chown -R 10001:10001 /var/lib/ursula /etc/ursula /tmp

USER 10001:10001
WORKDIR /var/lib/ursula
COPY --from=builder /usr/local/bin/ursula /usr/local/bin/ursula
COPY --from=builder /usr/local/bin/ursulactl /usr/local/bin/ursulactl
COPY --from=builder /usr/local/bin/ursulagw /usr/local/bin/ursulagw

EXPOSE 4437

ENTRYPOINT ["/usr/local/bin/ursula"]
CMD ["--listen", "0.0.0.0:4437", "--raft-memory"]
