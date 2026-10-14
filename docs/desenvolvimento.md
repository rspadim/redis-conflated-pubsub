# Desenvolvimento

## Toolchain

O projeto fixa a toolchain em
[`rust-toolchain.toml`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/rust-toolchain.toml)
e o CI usa Rust 1.99.0. O `Cargo.toml` declara `edition = "2024"` e
`rust-version = "1.88"`.

## Build e inspeção

Da raiz do repositório:

```sh
cargo build --release --locked
./target/release/redis-conflated-pubsub --help
./target/release/redis-conflated-pubsub --config-json-schema > config.schema.json
./target/release/redis-conflated-pubsub --check-config --config config.json
./target/release/redis-conflated-pubsub --config config.json
```

No Windows use `target\release\redis-conflated-pubsub.exe` com os mesmos
argumentos.

O schema gerado (draft 2020-12) cobre grupos de deduplicação por saída (`ttl_ms`,
`round_ms`, `restart_on_change`, `max_members`, `max_cache_bytes`), perfis
nomeados com chaves pontuadas e políticas de canal ordenadas com um seletor
(`glob`, `prefix`, `suffix` ou `default: true`) mais um perfil ou sobrescritas
inline.

## Testes

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

Os testes unitários e de integração ficam em `tests/unit/`, incluídos como módulos
do crate. Benchmarks manuais (ignorados) rodam com
`cargo test --release --locked <nome> -- --ignored --nocapture`; a lista e os
números de referência estão em [Arquitetura](design.md#benchmarks).

Os E2Es rodam por Compose; veja [Docker e testes](docker.md).

## Estrutura

O mapa de módulos e o comportamento do runtime estão em
[Arquitetura](design.md). Os documentos de design canônicos do repositório são
[`src/DESIGN.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/src/DESIGN.md)
(arquitetura e medições) e
[`DESIGN-0.3.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/DESIGN-0.3.md)
(proposta do backend NATS). O `CHANGELOG.md` na raiz é o histórico canônico.

## Documentação deste site

O site é construído com MkDocs Material e o plugin `mkdocs-static-i18n`.

```sh
pip install -r requirements-docs.txt
mkdocs serve          # http://127.0.0.1:8000
mkdocs build --strict # mesma validação usada no deploy
```

O conteúdo pt-BR fica em `docs/*.md` e é o idioma padrão, servido na raiz. O
deploy acontece pelo workflow `.github/workflows/docs.yml` a cada push para
`main`, publicando em GitHub Pages.

### Adicionar um idioma

Os arquivos são resolvidos pela convenção de sufixo do plugin: um arquivo
`docs/<pagina>.<locale>.md` substitui `docs/<pagina>.md` naquele idioma. Páginas
sem tradução caem para o idioma padrão (`fallback_to_default: true`).

1. Crie a tradução ao lado do original, por exemplo
   `docs/index.en.md` e `docs/inicio-rapido.en.md`.
2. Descomente (ou acrescente) o bloco do idioma em `mkdocs.yml`:

   ```yaml
   plugins:
     - i18n:
         languages:
           - locale: pt-BR
             name: Português (Brasil)
             default: true
             build: true
           - locale: en
             name: English
             build: true
   ```

3. Rode `mkdocs build --strict` e confira o seletor de idiomas no topo.

Use `nav_translations` se quiser traduzir os títulos do menu, e `admonition_translations`
para os títulos de avisos. O idioma `en` é servido em `/en/`.

!!! note "Renomear o repositório"
    Ao trocar o nome do repositório, atualize `site_url`, `repo_url`, `repo_name`
    e `edit_uri` em `mkdocs.yml`, além da badge e dos links do `README.md`.
