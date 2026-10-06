# CI and releases

Workflow definitions are in [`../.github/workflows/ci.yml`](../.github/workflows/ci.yml) and [`../.github/workflows/release.yml`](../.github/workflows/release.yml). Run local Docker/Compose commands from the repository root; see [Docker tests](../docker/README.md#compose-integration-tests).

## Continuous integration

The CI workflow runs on pushes to `main`, pull requests, and manual dispatch. It pins Rust 1.99.0 and checks formatting, Clippy with warnings denied, unit tests, and release builds on Ubuntu 22.04/24.04/26.04, Windows, and macOS. Additional jobs:

- Build the static Linux x86_64 musl image with the root `Dockerfile`, smoke-test `--version`, and upload the extracted binary as a workflow artifact; run that binary in Ubuntu 20.04/22.04/24.04/26.04 containers.
- Run Compose E2Es for Pub/Sub, fault handling, RESP limits/oversized-message policies, and TTL/conflation. Run the TTL stack command from the repository root, where `compose.ttl.test.yml` lives:

  ```sh
  docker compose -p redis-conflated-ci-ttl -f compose.ttl.test.yml up --build --remove-orphans --abort-on-container-exit --exit-code-from ttl-integration-test
  docker compose -p redis-conflated-ci-ttl -f compose.ttl.test.yml down --volumes --remove-orphans
  ```

- Validate monitoring with Python 3.13 unit tests, JSON parsing, and Zabbix XML parsing.

The TTL Compose E2E verifies output-local deduplication groups with shared expiry anchored to rounded start timestamps, both fixed (`restart_on_change: false`) and reset-on-change (`true`) modes, and that only published changes/new members reset a deadline. It exercises profile-based and inline channel rules, one default fallback policy and ordered first-match selection, plus independent 100/300 ms scheduler intervals. The TTL Compose command above is run from the repository root; Docker usage is in [`docker/README.md`](../docker/README.md).

## Tagged releases

Pushing a tag matching `v*` starts the release workflow. Use a version-aligned tag, for example:

```sh
git tag v0.1.3
git push origin v0.1.3
```

The workflow builds four targets and publishes the archives and SHA-256 sidecar files as GitHub Release assets:

| Target | Archive | Checksum |
| --- | --- | --- |
| Linux x86_64 musl | `linux-x86_64-musl.tar.gz` | `.tar.gz.sha256` |
| Windows x86_64 | `windows-x86_64.zip` | `.zip.sha256` |
| macOS Intel x86_64 | `macos-x86_64.tar.gz` | `.tar.gz.sha256` |
| macOS Apple Silicon | `macos-aarch64.tar.gz` | `.tar.gz.sha256` |

Each archive contains the corresponding service executable. The Linux release binary is built for the `x86_64-unknown-linux-musl` target; the separate CI image smoke test also extracts and tests this static binary.
