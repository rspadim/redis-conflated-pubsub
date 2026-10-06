# Developer build instructions

The project uses the Rust toolchain pinned in [`../rust-toolchain.toml`](../rust-toolchain.toml). From the repository root, build and inspect the binary, generate the draft 2020-12 schema, and validate a configuration example from [`../README.md`](../README.md). The schema covers output-local deduplication groups (`ttl_ms`, `round_ms`, `restart_on_change`, `max_members`, and `max_cache_bytes`), named profiles with literal dotted keys, and ordered channel policies with one selector (`glob`, `prefix`, `suffix`, or `default: true`) plus either a profile or inline overrides. Runtime validation enforces cross-field TTL bounds and requires the default rule last. The root README describes selection, cache capacity, and expiry semantics.

```sh
cargo build --release --locked
./target/release/redis-conflated-pubsub --help
./target/release/redis-conflated-pubsub --config-json-schema > config.schema.json
./target/release/redis-conflated-pubsub --check-config --config config.json
./target/release/redis-conflated-pubsub --config config.json
```

On Windows, use `target\release\redis-conflated-pubsub.exe` with the same arguments.
