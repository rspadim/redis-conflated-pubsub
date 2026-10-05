# syntax=docker/dockerfile:1.7
FROM rust:1.99.0-alpine3.22 AS builder

RUN apk add --no-cache build-base musl-dev perl

WORKDIR /build

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    rustup target add x86_64-unknown-linux-musl \
    && cargo build --release --locked --target x86_64-unknown-linux-musl \
    && mkdir -p /out \
    && cp target/x86_64-unknown-linux-musl/release/redis-conflated-pubsub /out/

FROM scratch

COPY --from=builder /out/redis-conflated-pubsub /usr/local/bin/redis-conflated-pubsub

ENTRYPOINT ["/usr/local/bin/redis-conflated-pubsub"]
