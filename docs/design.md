# Arquitetura

Este documento descreve o runtime atual, os padrões de configuração e as
medições de referência. Para o histórico de mudanças veja
[Histórico de mudanças](changelog.md); a proposta original do backend NATS está em
[`DESIGN-0.3.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/DESIGN-0.3.md).

## Propósito

Encaminhar mensagens Pub/Sub cruas de **uma** entrada para um ou mais saídas
nomeadas. A entrada é Redis (`SUBSCRIBE`/`PSUBSCRIBE`) **ou** Core NATS
(`nats_subject`); cada saída escolhe Redis ou Core NATS de forma independente.

Cada saída mapeia endereços, filtra mensagens, conflata para o último valor por
canal e intervalo, deduplica payloads idênticos dentro de um TTL e publica no seu
próprio endpoint. Payloads continuam bytes crus; não há spool em disco.

## Backends

A seleção de backend é agnóstica de protocolo: `input` e cada saída carregam
exatamente uma de duas chaves opcionais, `redis` ou `nats`. O JSON existente de
Redis não muda.

- `--check-config` e o JSON Schema gerado impõem a regra de exatamente um backend
  (o schema via `oneOf` sobre `required: ["redis"]`/`["nats"]`).
- Cruzamentos que só a validação de runtime faz: tipo de assinatura compatível com
  o backend de entrada, regra de uma família de transporte por lista e arquivos
  mTLS pareados.

Redis usa `redis-rs`; Core NATS usa `async-nats` (features `websockets`, `nkeys`,
`ring`). Os dois backends compartilham o caminho de filtragem/fila/conflation/
deduplicação e diferem apenas no leitor de entrada e no destino de saída.

## Mapa de módulos

| Caminho | Responsabilidade |
| --- | --- |
| `main.rs` | CLI, carregamento de config, `--check-config`, impressão do schema, sinais. |
| `service.rs` | Fachada do ciclo de vida: compila a config, sobe workers, supervisiona reload/shutdown. |
| `service/input.rs` | Leitor Pub/Sub, exclusão de Sentinel/eco, filtros de entrada, casamento de assinaturas e fan-out. |
| `service/output.rs` | Loop do worker de saída: intake, resolução de política, agendadores por intervalo, janela direta em voo. |
| `service/output/flush.rs` | Liquidação de fila, descarga de bucket conflado, caminho de publicação direta em lote. |
| `service/batch.rs` | Contabilização de frames RESP, tamanho de lote, política de mensagem grande. |
| `service/publish.rs` | Conexão Redis, comandos de publish, classificação de falha; seleção de `OutputSink`/`OutputConnection`. |
| `service/backend.rs` | Fronteira de backend: `InputEnvelope` e as traits `InputSource`/`OutputSink`, mais a constante do header de hop. |
| `service/nats/connect.rs` | Transforma um `NatsConfig` em cliente conectado; separa o `ConnectionPlan` sem segredos do `connect` que resolve segredos. |
| `service/nats/input.rs` | Leitor de entrada NATS: instala assinaturas, faz fan-in dos streams, descarta reentrada de loop. |
| `service/nats/output.rs` | Sink de saída NATS: publish ordenado mais um `flush`; carimba o header de hop. |
| `service/policy.rs`, `service/policy/glob.rs` | Seletores de canal compilados, cache de resolução de política, agendadores. |
| `service/dedup.rs` | Cache TTL por canal e grupos de deduplicação nomeados. |
| `service/filter.rs`, `service/channel_cache.rs` | Filtros ordenados e as LRUs limitadas por trás de filtros/políticas, com inspeção opt-in. |
| `config.rs`, `config/schema.rs`, `config/validate.rs` | Config Serde, JSON Schema gerado, validação semântica. |
| `status.rs`, `http_status.rs`, `logging.rs` | Snapshot de status, endpoints HTTP opcionais, logs JSON com limite de tamanho. |

## Runtime

`service::run` compila a configuração uma vez por start/reload e então supervisiona:

- uma **task de entrada** que lê a conexão Pub/Sub;
- uma **task de worker de saída** por saída configurada, cada uma com sua conexão
  Redis, cache de política, cache de deduplicação e agendadores de intervalo;
- escritor de status e listener HTTP opcionais.

### Caminho de entrada

O único leitor de entrada processa mensagens na ordem de chegada:

1. Descarta canais de notificação do Sentinel e ecos de saída quando configurado
   (ambos padrão ligado; ambos específicos de Redis).
2. Casa a mensagem com as assinaturas configuradas.
3. Aplica o **filtro de entrada** ao canal de origem.
4. Para cada saída: aplica o **filtro** daquela saída ao canal mapeado pela
   assinatura (antes do prefixo/sufixo da própria saída), monta o canal final e
   envia uma cópia para a fila da saída.

Para entrada Redis o leitor é `service/input.rs`; para NATS é
`service/nats/input.rs` (`read_nats_input`), que instala cada assinatura
`nats_subject`, faz o fan-in dos streams em round-robin, carrega a identidade da
assinatura que casou para dentro do mapeamento e descarta qualquer mensagem já
carimbada com o header de hop (loop guard). A exclusão de Sentinel/eco é só de
Redis; o caminho NATS conta com o header de hop.

### Worker de saída

Cada worker é um consumidor único e ordenado. Ele resolve a política de canal à
medida que as mensagens são aceitas e então roteia por intervalo:

**Lane direta (`interval_ms <= 0`)** — quando a deduplicação está desligada para o
canal, as mensagens passam por um deque de passthrough e são publicadas por uma
janela limitada em voo:

- Os limites são `max_commands_per_exec` comandos e o alvo de bytes RESP
  `max_bytes_per_exec`.
- Itens diretos já enfileirados são enviados juntos, ordenados, dentro de
  `MULTI`/`EXEC`; um item sozinho é publicado imediatamente.
- As respostas são coletadas em ordem; entradas enfileiradas seguem contadas como
  pendentes até seu comando liquidar.
- Mensagens cujo canal ou grupo de deduplicação já tem entrada em voo esperam
  esses resultados primeiro, para que a supressão por TTL compare com o desfecho
  final da publicação.

**Lane conflada (`interval_ms > 0`)** — valores pendentes ficam em
`interval_ms → canal final → última mensagem`. Existe um timer por intervalo
distinto presente na configuração; a cada tick o worker descarrega **apenas**
aquele bucket, em ordem de canal, em chunks `MULTI`/`EXEC` limitados. Substituir
um valor pendente conta como conflation; a deduplicação é checada imediatamente
antes de cada chunk ser enviado.

Timers mantêm a cadência configurada; um worker ocupado publicando não acumula
ticks perdidos.

### Publicação e falhas

- O batching usa os tamanhos exatos dos frames RESP, incluindo `MULTI`/`EXEC`.
- Mensagens grandes seguem `oversized_message_policy`: `send` (padrão), `truncate`
  (sufixo do payload) ou `drop`; a decisão é aplicada por saída quando a mensagem
  é aceita.
- **Falhas não são retentadas.** Falhas `NotSent` contam como descartadas; falhas
  após o envio (resposta de erro, timeout) contam como incertas e o lote é
  abandonado. A conexão é descartada na falha e restabelecida na próxima
  publicação.
- Publicações incertas são lembradas pelo cache de deduplicação, porque podem ter
  alcançado assinantes.

### Deduplicação

A deduplicação por canal compara o canal de saída exato e os bytes do payload de
entrada cru (inclusive bytes que uma política de truncate removeu). É um TTL de
diferença: só payloads idênticos consecutivos são suprimidos, e só até a janela
expirar. Veja [Configuração](configuracao.md#ttl-de-diferenca) para as regras
completas, grupos e o modo `in_flight_suppression`.

### Limites de fila

A fila de intake de cada saída é ilimitada por padrão. `queue_max_messages`/
`queue_max_bytes` a limitam (bytes contam nome do canal mais payload) com
`queue_overflow_policy`:

- `drop_newest` (padrão): o fan-out recusa a mensagem nova quando um limite é
  excedido;
- `drop_oldest`: usa uma fila compartilhada, evictando a entrada mais antiga para
  admitir a nova;
- `drop_by_age`: o worker descarta mensagens mais velhas que `queue_max_age_ms` no
  intake (sem limite de item/byte).

Bloquear o leitor **não** é oferecido porque o Redis iria bufferizar e depois
desconectar o assinante. Como Pub/Sub não tem replay, qualquer descarte perde
eventos explicitamente.

### Reload e shutdown

No Unix, o `SIGHUP` faz parse e valida o arquivo novo antes de trocar; um arquivo
inválido deixa a configuração em execução intacta. Um reload aceito drena as filas
pendentes e reinicia os workers de entrada/saída em processo, o que cria uma
breve lacuna na entrada e zera os contadores de status. Mudanças de logging ainda
exigem reinício completo; uma mudança de `instance_lock.path` é adquirida durante
o reload (o novo lock antes de liberar o antigo) e o reload é rejeitado se o novo
lock não estiver disponível. O shutdown para a entrada, drena os workers e
descarrega os buckets conflados restantes.

## Observabilidade

O snapshot de status (`schema_version` 5) expõe contadores globais e por saída
para entrada, publicadas/confladas/deduplicadas/descartadas/truncadas/shed e seus
bytes, evicções de deduplicação, erros de publish, operações incertas, reconexões,
mensagens suprimidas pelo loop guard (NATS) e pendências em mensagens/bytes/chaves.
Por saída também expõe:

- **espera na fila** — amostras/total/máximo do enfileiramento no fan-out até a
  aceitação pelo worker (inclui resolução de política);
- **RTT de publish** — amostras/total/máximo em volta do round trip de publicação;
- gauges de **bytes de fila pendente** e **idade do pendente mais antigo**.

Eventos de conexão NATS são contados globalmente
(`nats_client_connects_total`, `nats_client_disconnects_total`) e descartes por
consumidor lento (`nats_client_slow_consumers_total`). Como o Core NATS reconecta
internamente, um failover de nó **não** aparece nos contadores de reconnect; esses
contadores são best-effort (o `async-nats` pode perder eventos em tempestade de
reconexão, o que subconta, nunca conta duas vezes).

`GET /filters` expõe nomes de canal completos e entradas de cache e fica desligado
por padrão, porque revela nomes crus; habilitá-lo troca as LRUs de filtro/política
por armazenamento compartilhado protegido por mutex.

## Padrões de configuração

| Configuração | Padrão |
| --- | --- |
| `outputs.<name>.conflation.interval_ms` | obrigatório |
| `conflation.max_commands_per_exec` | `256` |
| `max_bytes_per_exec` / override por saída | `4 MiB` |
| `oversized_message_policy` | `send` |
| `outputs.<name>.deduplication.ttl_ms` | `0` (desligado) |
| `deduplication.in_flight_suppression` | `false` |
| Limites de TTL por canal (`max_entries`/`max_cache_bytes`) | não definido (ilimitado) |
| Limites de grupo de deduplicação | `16384` membros / `64 MiB` |
| Limites de fila (`queue_max_messages`/`queue_max_bytes`/`queue_max_age_ms`) | não definido (ilimitado), `drop_newest` |
| Capacidade de cache de filtro/política (cada) | `16384`, máx `100000`, `0` desliga |
| `exclude_output_echoes`, `exclude_sentinel_pubsub` | `true` |
| `logging.prefix` / `logging.enabled` | `redis-conflated-pubsub` / `true` |
| Template `client_name` | `ConflatedPS-{version}-{role}::{user}::{host}` |
| Intervalo de atualização de status | `1000 ms` |
| NATS `connect_timeout_ms` / `auth` / `ignore_discovered_servers` | `5000` / `none` / `false` |

Intervalos e TTLs positivos são limitados a 365 dias; o `round_ms` de um grupo não
pode exceder um TTL de grupo positivo.

## Benchmarks

### Hot path ponta a ponta

`examples/hotpath_benchmark.rs` roda todos os publishers e ambos os assinantes de
saída em um único processo Rust. Ele aquece 64 canais, mede 250 requisições
seriais e então roda a rajada concorrente, reportando percentis seriais, uma
decomposição por fase (RTT do ACK de entrada, tempo de serviço pós-ACK e
percentis ponta a ponta por saída) e o pico de backlog de pendências.

`HOTPATH_MODE=closed-loop` (padrão) espera o ack de cada `PUBLISH`;
`HOTPATH_MODE=open-loop` envia as mensagens em pipelines não atômicos de
`HOTPATH_PIPELINE` comandos (padrão 64) por `HOTPATH_CONNECTIONS` conexões por
publisher, sem esperar acks individuais, para forçar pressão sustentada. Comandos
e comandos de medição completos em [Docker e testes](docker.md).

### Captura e replay

`capture-feed` grava metadados de Pub/Sub somente leitura (timestamp relativo,
canal e tamanho do payload; **nunca** o conteúdo) em NDJSON local mais um resumo.
`replay-feed` republica uma captura preservando o timing de interchegada gravado,
com payloads sintéticos do tamanho registrado e repetição opcional por canal para
exercitar o TTL, para que formatos realistas de feed dirijam testes de carga sem
exportar dados da aplicação.

Uma captura de 30 minutos de um feed de market data (6.054.668 mensagens,
3.364 msg/s de média, 5.508 msg/s de pico, 8.664 canais únicos) reproduzida contra
o stack local com conflation de 200 ms, TTL de 5 s e a janela recomendada
1024/batch 64:

| Replay | Entrada | Publicadas | Pendentes | Espera na fila média/máx | RTT de publish médio/máx |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1× (30 min) | 6.053.786 | 2.292.837 (37,9%) | 0 | 0,071/13,5 ms | 0,492/11,7 ms |
| 4× | 6.053.786 | 1.371.471 (22,7%) | 0 | 0,080/5,1 ms | 0,443/6,9 ms |
| 8× | 6.053.786 | 1.127.642 (18,6%) | 0 | 0,095/8,2 ms | 0,462/8,2 ms |
| 4×, janela/batch 256/256 | 6.053.786 | 1.357.014 (22,4%) | 0 | 0,104/11,6 ms | 0,975/8,0 ms |
| 4×, payloads repetidos | 6.053.786 | 979.805 (16,2%) | 0 | 0,090/9,2 ms | 0,448/8,0 ms |

A entrada exclui 882 mensagens `__sentinel__:hello` por execução. A conflation
reduziu a publicação para 16–38% da entrada conforme a taxa, com zero backlog e
espera de fila e RTT de publish sub-milissegundo em média.

### Loopback (sem Redis/Valkey)

`examples/loopback_benchmark.rs` implementa um broker RESP2 mínimo em Rust: aceita
o `PSUBSCRIBE` do serviço, alimenta mensagens tão rápido quanto o socket aceita e
atua como o servidor de saída recebendo `PUBLISH`/`MULTI`/`EXEC`, reportando
percentis ponta a ponta. `compose.loopback.test.yml` roda isso sem containers
Redis, isolando o serviço e o protocolo.

### Comparação de transporte e e2e NATS

`spikes/compare` é um crate Rust separado que mede os caminhos de transporte crus
(`async-nats` vs `redis-rs`) e dirige o **binário real do relay** ponta a ponta.
Seu publisher e subscriber rodam como **processos separados** para que o
bufferização do publisher não infle a latência medida; um harness de processo
único produz números dominados por fila e não deve ser usado para alegações de
latência.

### Medições de referência

Execuções locais em Docker/WSL com nomes sintéticos de 64 canais, caches locais ao
worker e servidores em modo Pub/Sub puro. Trate-as como indicativas, não como
comparações controladas de engine.

Sweep de janela (`max_commands_per_exec`, uma saída, 640k, Redis 7.4.11, pipeline
128): 256 → 99,8k msg/s; 1024 → 85,6k msg/s; 4096 → 93,8k msg/s. A taxa não escala
com a janela em voo enquanto o RTT de publish cresce proporcionalmente, então o
platô é o processamento de `PUBLISH` no servidor, não a janela do cliente.

Grade 2D após o desacoplamento (uma saída, 640k, open-loop, mediana de 3 reps):

| Batch | Janela | Taxa (msg/s) | Espera na fila média | RTT de publish médio |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 256 | 116,1k | 1566 ms | 2,0 ms |
| 64 | 1024 | 129,3k | 848 ms | 6,9 ms |
| 64 | 4096 | 114,8k | 800 ms | 30,8 ms |
| 256 | 256 | 130,5k | 1225 ms | 1,6 ms |
| 256 | 1024 | 129,3k | 898 ms | 6,6 ms |
| 256 | 4096 | 117,3k | 655 ms | 27,5 ms |
| 1024 | 256 | 130,6k | 1396 ms | 1,7 ms |
| 1024 | 1024 | 123,0k | 979 ms | 7,0 ms |
| 1024 | 4096 | 119,8k | 712 ms | 21,3 ms |

A janela ajuda até 1024; em 4096 o RTT explode porque muitos `MULTI`/`EXEC`
concorrentes enfileiram no servidor de saída. A melhor região é janela 1024 com
batch 64–256. A variância entre execuções é alta, então diferenças de taxa abaixo
de ~10% são ruído; os sinais de latência são os robustos.

Após as otimizações (payload `Arc<[u8]>`, janela em voo desacoplada, fairness de
flush por turno), execuções com Redis 7.4.11: uma saída 640k — **139,4k msg/s**
com a janela padrão e **149,1k** com `max_in_flight_commands: 1024`; duas saídas
640k — **77,6k msg/s** (~155k publishes/s), igual com um servidor ou dois; loopback
(sem Redis) 1M — **80k msg/s**. Antes dessas mudanças as mesmas execuções mediam
99,8k (uma), 50,1k (duas) e 57,6k (endpoints separados).

Observações:

- O termo que cresce em 640k é a **espera na fila de intake** (fan-out até
  aceitação pelo worker): o leitor único de entrada mais o intake por worker é a
  próxima alavanca de concorrência.
- Separação de endpoints: com uma saída o cenário é limitado pelo servidor perto
  de 90–100k publishes/s; com duas saídas o platô mistura processamento do
  servidor e um teto de cliente/intake em torno de 50–58k entradas/s.
- Variância entre execuções é alta: a mesma configuração de uma saída mediu 139,4k,
  mediana de 129k na grade e 106,9k na verificação, com amostras cruas de
  91k–138k. Prefira os sinais de latência (`queue_wait`, RTT de publish).

## Suíte de testes

- Testes unitários e de integração ficam em `tests/unit/`, incluídos como módulos
  do crate; `cargo test --locked` passa 187 testes no Windows e 198 no Unix (onde
  os testes de reload `#[cfg(unix)]` compilam), e lista 8 benchmarks manuais
  ignorados.
- Compose E2Es (veja [Docker e testes](docker.md)): integração Pub/Sub, mensagens
  grandes, protocolo RESP/políticas, TTL/grupos/SIGHUP, latência de hot path e
  tratamento de falhas.
- `cargo fmt --all --check` e
  `cargo clippy --all-targets --locked -- -D warnings` devem passar limpos.

Benchmarks unitários manuais: rode com
`cargo test --release --locked <nome> -- --ignored --nocapture`.

- `filter_cache_load_benchmark`, `channel_policy_cache_load_benchmark`
  (`tests/unit/service/filter_load.rs`) — 64 regras, conjunto quente de 8.192
  canais, churn de 24.576 canais, mais o replay do fixture anonimizado 100×.
  Misses com 64 regras: ~30–41 µs (filtros) e ~28–39 µs (políticas); hits
  ~80–120 ns.
- `direct_lane_load_benchmark` (`tests/unit/service/direct_lane_load.rs`) — drena
  100.000 mensagens diretas enfileiradas com latência simulada por comando (padrão
  50 µs) em batch caps 1/32/256 (836 / 11.443 / 18.187 msg/s) e garante ordem FIFO.
