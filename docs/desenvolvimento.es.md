# Desarrollo

## Toolchain

El proyecto fija la toolchain en
[`rust-toolchain.toml`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/rust-toolchain.toml)
y el CI usa Rust 1.99.0. El `Cargo.toml` declara `edition = "2024"` y
`rust-version = "1.88"`.

## Build e inspección

Desde la raíz del repositorio:

```sh
cargo build --release --locked
./target/release/redis-conflated-pubsub --help
./target/release/redis-conflated-pubsub --config-json-schema > config.schema.json
./target/release/redis-conflated-pubsub --check-config --config config.json
./target/release/redis-conflated-pubsub --config config.json
```

En Windows usa `target\release\redis-conflated-pubsub.exe` con los mismos
argumentos.

El schema generado (draft 2020-12) cubre grupos de deduplicación por salida (`ttl_ms`,
`round_ms`, `restart_on_change`, `max_members`, `max_cache_bytes`), perfiles
nombrados con claves punteadas y políticas de canal ordenadas con un selector
(`glob`, `prefix`, `suffix` o `default: true`) más un perfil o sobrescrituras
inline.

## Pruebas

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

Las pruebas unitarias y de integración están en `tests/unit/`, incluidas como módulos
del crate. Los benchmarks manuales (ignorados) se ejecutan con
`cargo test --release --locked <nombre> -- --ignored --nocapture`; la lista y los
números de referencia están en [Arquitectura](design.md#benchmarks).

Los E2Es corren por Compose; ver [Docker y pruebas](docker.md).

## Estructura

El mapa de módulos y el comportamiento del runtime están en
[Arquitectura](design.md). Los documentos de diseño canónicos del repositorio son
[`src/DESIGN.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/src/DESIGN.md)
(arquitectura y mediciones) y
[`DESIGN-0.3.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/DESIGN-0.3.md)
(propuesta del backend NATS). El `CHANGELOG.md` en la raíz es el historial canónico.

## Documentación de este sitio

El sitio se construye con MkDocs Material y el plugin `mkdocs-static-i18n`.

```sh
pip install -r requirements-docs.txt
mkdocs serve          # http://127.0.0.1:8000
mkdocs build --strict # misma validación usada en el deploy
```

El contenido pt-BR está en `docs/*.md` y es el idioma predeterminado, servido en la
raíz. El deploy ocurre mediante el workflow `.github/workflows/docs.yml` en cada push
a `main`, publicando en GitHub Pages.

### Ayuda a traducir

El sitio se publica en portugués (predeterminado, en la raíz), inglés (`/en/`) y
español (`/es/`). Otros idiomas son bienvenidos — y las correcciones sobre los
existentes también.

Los archivos se resuelven por la convención de sufijo del plugin: un archivo
`docs/<pagina>.<locale>.md` reemplaza a `docs/<pagina>.md` en ese idioma. Las
páginas sin traducción caen al idioma predeterminado
(`fallback_to_default: true`), así que la traducción puede entrar página por
página.

1. Abre un
   [issue de traducción](https://github.com/rspadim/redis-conflated-pubsub/issues/new?template=translation.yml)
   (o comenta en uno existente) para que el idioma quede registrado.
2. Crea la traducción junto al original — por ejemplo `docs/index.de.md` y
   `docs/inicio-rapido.de.md` para alemán (`de`).
3. Agrega el bloque del idioma en `mkdocs.yml`:

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

   `nav_translations` traduce los títulos del menú; `admonition_translations` hace
   lo mismo con los títulos de avisos. El nuevo idioma se sirve en `/de/`.

4. Ejecuta `mkdocs build --strict` y revisa el selector de idiomas en la parte
   superior.

Lo que **nunca** se traduce: nombres de campos JSON, comandos de shell, nombres de
métricas, rutas de archivos, mensajes de error, versiones y números. Mantén el
orden de las secciones de la fuente en pt-BR y enlaza las páginas por la ruta
**sin** el sufijo de idioma (`nats.md`, nunca `nats.de.md`).

!!! note "Renombrar el repositorio"
    Al cambiar el nombre del repositorio, actualiza `site_url`, `repo_url`, `repo_name`
    y `edit_uri` en `mkdocs.yml`, además de la badge y los enlaces del `README.md`.
