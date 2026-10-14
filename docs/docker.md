# Docker e testes

Rode todos os comandos **da raiz do repositório**: `Dockerfile` e os
`compose.*.yml` ficam lá e os caminhos dos exemplos são relativos a ela.

## Build e execução

Construa a imagem Linux e rode com uma configuração montada e um diretório de
estado gravável:

```sh
docker build --tag redis-conflated-pubsub:local .
cp config.example.json config.json
# Edite config.json para que os endereços Redis sejam alcançáveis do container.
mkdir -p state
docker run --rm \
  --workdir /state \
  --volume "$PWD/config.json:/config.json:ro" \
  --volume "$PWD/state:/state" \
  redis-conflated-pubsub:local \
  --config /config.json
```

Monte a config de runtime em `/config.json`; a imagem usa `scratch` e não contém
shell nem arquivo de configuração. Caminhos relativos de lock, status e log
resolvem a partir de `/state` neste exemplo. Para expor o endpoint de status,
configure o bind como `0.0.0.0` e publique a porta, por exemplo `--publish 9090:9090`.

## Imagem única (bundle) com Redis/Valkey

O `Dockerfile.bundle` opcional contém o relay, o Redis e o Valkey 9.1.2. Em
runtime, `KV_ENGINE` seleciona a implementação (`redis` por padrão, ou `valkey`); a
engine escolhida roda **duas** instâncias: crua na porta `6379` e conflada na
`6380`. A config do relay é montada em `/etc/redis-conflated/config.json`;
`config.bundle.example.json` é o ponto de partida. São três processos em um
container; o `Dockerfile` normal continua sendo a imagem só do relay.

```sh
cp config.bundle.example.json config.bundle.json
docker build -f Dockerfile.bundle -t redis-conflated-pubsub:bundle .
docker run -d --name redis-conflated-bundle --restart unless-stopped \
  -e KV_ENGINE=redis \
  -e REDIS_PASSWORD='replace-with-a-secret' \
  -p 6379:6379 -p 6380:6380 \
  -v "$PWD/config.bundle.json:/etc/redis-conflated/config.json:ro" \
  -v redis-raw-data:/data/raw \
  -v redis-conflated-data:/data/conflated \
  -v redis-relay-state:/state \
  redis-conflated-pubsub:bundle
```

Use `KV_ENGINE=valkey` para o Valkey. `REDIS_PASSWORD` é obrigatório;
alternativamente monte um secret e defina `REDIS_PASSWORD_FILE`. As portas
publicadas escutam em todas as interfaces; para acesso só do host use
`-p 127.0.0.1:6379:6379 -p 127.0.0.1:6380:6380`. Use volumes de dados separados ao
trocar de engine, a menos que tenha verificado a compatibilidade dos arquivos.

## Testes de integração (Compose)

Cada teste cria containers, redes e volumes com escopo de projeto; não há nomes
fixos de container, IPs ou sub-redes. Rode o `up` da raiz e o `down`
correspondente depois. Para execuções em paralelo, passe um `-p <projeto>` único.

| Stack | O que verifica |
| --- | --- |
| `compose.test.yml` | Mapeamento de canais, Pub/Sub entre databases, conflation, payloads binários, prevenção de eco e status HTTP. |
| `compose.filters.test.yml` | Filtros ordenados de entrada/saída, mapeamento de namespace e snapshot de cache do `GET /filters`. |
| `compose.oversize.test.yml` | Limite de 1 MiB do query-buffer do Redis e tratamento de chunk falho. |
| `compose.protocol.test.yml` | Transações por byte, políticas `send`/`truncate`/`drop` e `PUBLISH` direto sem transação. |
| `compose.ttl.test.yml` | Supressão de duplicatas, TTL individual e agrupado, seleção de política, `SIGHUP` válido e inválido. |
| `compose.test.yml --profile fault` | Tratamento de publish incerto/falho via proxy de falha em Rust. |

```sh
docker compose -f compose.test.yml up --build --abort-on-container-exit --exit-code-from integration-test
docker compose -f compose.test.yml down --volumes --remove-orphans
```

Os drivers e proxies vêm de `examples/e2e.rs` (binário `redis-conflated-e2e`,
construído por `Dockerfile.e2e`), substituindo os antigos scripts Python.

## Hot path ponta a ponta

O benchmark de hot path (`examples/hotpath_benchmark.rs`, imagem
`Dockerfile.benchmark`) roda publishers e ambos os assinantes de saída em um
processo Rust. A imagem padrão é Valkey 9.1.2.

```sh
docker compose -p hotpath-valkey -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
docker compose -p hotpath-valkey -f compose.hotpath.test.yml down --volumes --remove-orphans
```

Para o Redis, selecione imagem e CLI explicitamente:

```sh
HOTPATH_KV_IMAGE=redis:7.4-alpine HOTPATH_KV_CLI=redis-cli \
docker compose -p hotpath-redis -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
```

Variáveis de ajuste:

| Variável | Efeito | Padrão |
| --- | --- | --- |
| `HOTPATH_PUBLISHER_COUNT` | Número de publishers concorrentes. | — |
| `HOTPATH_MESSAGES_PER_PUBLISHER` | Mensagens por publisher. | — |
| `HOTPATH_SERIAL_MESSAGES` | Requisições seriais medidas. | 250 |
| `HOTPATH_MODE` | `closed-loop` (espera cada ACK) ou `open-loop` (pipelines não atômicos). | `closed-loop` |
| `HOTPATH_PIPELINE` | Comandos por pipeline no open-loop. | 64 |
| `HOTPATH_CONNECTIONS` | Conexões por publisher no open-loop. | 1 |
| `HOTPATH_OUTPUTS` | Uma ou duas saídas. | 2 |
| `HOTPATH_MIN_NUMPAT` | Nível de `PUBSUB NUMPAT` esperado antes de publicar (para servidores compartilhados). | 1 |

O workload de política mista (cerca de um terço direto, um terço conflation de
200 ms e um terço 200 ms + TTL de 5 s) usa outro harness, selecionado por
`HOTPATH_BENCHMARK_CMD='hotpath-mix-benchmark'`:

```sh
HOTPATH_CONFIG_FILE=./tests/docker-hotpath-mix-config.json \
HOTPATH_BENCHMARK_CMD='hotpath-mix-benchmark' \
docker compose -p hotpath-mixed -f compose.hotpath.test.yml up --build --abort-on-container-exit --exit-code-from hotpath-benchmark
```

## Loopback (sem Redis/Valkey)

Roda o serviço contra um broker RESP2 mínimo implementado em Rust, sem containers
Redis:

```sh
docker compose -p loopback -f compose.loopback.test.yml up --build --abort-on-container-exit --exit-code-from loopback-benchmark
```

`LOOPBACK_MESSAGES` (padrão 1.000.000), `LOOPBACK_CHUNK`, `LOOPBACK_WARMUP` e
`LOOPBACK_TIMEOUT_S` ajustam a execução. Como o broker divide CPU com o serviço,
trate os números como medições de serviço + broker.

## Captura e replay de feed

A imagem de benchmark também traz `capture-feed` e `replay-feed`:

- `capture-feed` assina em modo somente leitura (`CAPTURE_PATTERN`, padrão `*`) e
  grava **apenas metadados** por mensagem: timestamp relativo, canal e tamanho do
  payload. O conteúdo nunca é escrito. Emite NDJSON mais um resumo JSON (taxa,
  série por segundo, buckets de tamanho, top canais).
- `replay-feed` republica uma captura em um Redis alvo (`REPLAY_HOST`/`REPLAY_PORT`)
  preservando o timing de interchegada, com escala de taxa (`REPLAY_RATE_SCALE`),
  chunks em pipeline (`REPLAY_PIPELINE`), payloads sintéticos do tamanho gravado e
  repetição opcional por canal (`REPLAY_DUP_EVERY`) para exercitar o TTL. Com
  `REPLAY_STATUS_URL` imprime os contadores de fila/publish antes e depois.

Arquivos de captura ficam locais (`tests/.tmp-*` é ignorado pelo Git); apenas
agregados anonimizados devem ser commitados.

## CI e releases

Os workflows estão em `.github/workflows/`. O `ci.yml` roda em pushes para `main`,
pull requests e dispatch manual: formato, Clippy com warnings como erro, testes
unitários e build de release em várias plataformas, além de imagem estática
Linux, E2Es Compose, harnesses de spike e validação do monitoramento. O
`release.yml` dispara em tags `v*`, confere que a tag casa com a versão do
`Cargo.toml` e publica os quatro alvos com seus `.sha256`.
