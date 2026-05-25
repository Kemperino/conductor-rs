ARG RUST_VERSION=1.95
ARG DEBIAN_VERSION=bookworm

FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release --bin op-conductor && \
    cp /app/target/release/op-conductor /usr/local/bin/op-conductor

FROM debian:${DEBIAN_VERSION}-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates gawk netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/op-conductor /usr/local/bin/op-conductor

CMD ["op-conductor"]
