# Development

## Toolchain

The project pins the toolchain in
[`rust-toolchain.toml`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/rust-toolchain.toml)
and CI uses Rust 1.99.0. `Cargo.toml` declares `edition = "2024"` and
`rust-version = "1.88"`.

## Build and inspection

From the repository root:

```sh
cargo build --release --locked
./target/release/redis-conflated-pubsub --help
./target/release/redis-conflated-pubsub --config-json-schema > config.schema.json
./target/release/redis-conflated-pubsub --check-config --config config.json
./target/release/redis-conflated-pubsub --config config.json
```

On Windows use `target\release\redis-conflated-pubsub.exe` with the same
arguments.

The generated schema (draft 2020-12) covers output-local deduplication groups
(`ttl_ms`, `round_ms`, `restart_on_change`, `max_members`, `max_cache_bytes`),
named profiles with dotted keys and ordered channel policies with one selector
(`glob`, `prefix`, `suffix` or `default: true`) plus a profile or inline
overrides.

## Tests

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

Unit and integration tests live in `tests/unit/`, included as crate modules.
Manual (ignored) benchmarks run with
`cargo test --release --locked <name> -- --ignored --nocapture`; the list and the
reference numbers are in [Architecture](design.md#benchmarks).

The E2Es run through Compose; see [Docker and tests](docker.md).

## Structure

The module map and the runtime behavior are in
[Architecture](design.md). The repository's canonical design documents are
[`src/DESIGN.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/src/DESIGN.md)
(architecture and measurements) and
[`DESIGN-0.3.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/DESIGN-0.3.md)
(NATS backend proposal). The `CHANGELOG.md` at the root is the canonical history.

## Documentation for this site

The site is built with MkDocs Material and the `mkdocs-static-i18n` plugin.

```sh
pip install -r requirements-docs.txt
mkdocs serve          # http://127.0.0.1:8000
mkdocs build --strict # same validation used in the deploy
```

The pt-BR content lives in `docs/*.md` and is the default language, served at the
root. The deploy happens through the `.github/workflows/docs.yml` workflow on
every push to `main`, publishing to GitHub Pages.

### Help translate

The site ships in Portuguese (default, at the root), English (`/en/`) and Spanish
(`/es/`). More languages are welcome — and so are fixes to the existing ones.

Files are resolved by the plugin's suffix convention: a
`docs/<page>.<locale>.md` file replaces `docs/<page>.md` in that language. Pages
without a translation fall back to the default language
(`fallback_to_default: true`), so a translation can land one page at a time.

1. Open a
   [translation issue](https://github.com/rspadim/redis-conflated-pubsub/issues/new?template=translation.yml)
   (or comment on an existing one) so the locale is tracked.
2. Create the translation next to the original — for example `docs/index.de.md`
   and `docs/inicio-rapido.de.md` for German (`de`).
3. Add the locale block in `mkdocs.yml`:

   ```yaml
   plugins:
     - i18n:
         languages:
           - locale: de
             name: Deutsch
             build: true
             nav_translations:
               Início: Startseite
               Início rápido: Schnellstart
   ```

   `nav_translations` translates the menu titles; `admonition_translations` does
   the same for admonition titles. The new language is served at `/de/`.

4. Run `mkdocs build --strict` and check the language selector at the top.

What is never translated: JSON field names, shell commands, metric names, file
paths, error strings, versions and numbers. Keep the section order of the pt-BR
source and link between pages using the path **without** the locale suffix
(`nats.md`, never `nats.de.md`).

!!! note "Renaming the repository"
    When changing the repository name, update `site_url`, `repo_url`,
    `repo_name` and `edit_uri` in `mkdocs.yml`, plus the badge and the links in
    `README.md`.
