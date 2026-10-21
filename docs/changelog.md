# Histórico de mudanças

O arquivo canônico é o [`CHANGELOG.md`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/CHANGELOG.md)
na raiz do repositório. Abaixo está um resumo das versões.

## 0.4.0 — 2026-10-21

- The installer's "Test connection" button now runs the relay's own `--test-connection` against a draft of the configuration the wizard would write, instead of a PowerShell probe of the wire protocol. The new flag validates the configuration, then connects to every configured backend (Redis: a ladder of reads - `DBSIZE`, `PUBSUB CHANNELS`, `PING`; NATS: the full connection plan) and exits non-zero when any of them did not answer. It reuses the runtime's client configuration, so the check cannot disagree with what the service does at startup - including TLS, ACL, cluster and the NATS credential styles. Read-only: it connects and drops, publishing nothing and touching no key.
- The probe climbs a ladder of READ commands and treats any server reply as "connected". `PING` was the verdict and a Redis with `stop-writes-on-bgsave-error yes` answers it with `-MISCONF` while serving every read, so a usable server was reported unreachable. The ladder covers the ACL categories a restricted user may be granted separately (`@read`, `@pubsub`, `@connection`), and the verdict is "connected and authenticated": a server reply (MISCONF, NOPERM, READONLY, ERR) means the connection works, while an authentication failure is reported as such rather than as "unreachable". It never writes or publishes.
- The probe is available outside the installer too: `redis-conflated-pubsub --config config.json --test-connection` reports one line per backend (`input redis  host:port  OK`, or `FAILED: <reason>`), which makes it usable from a deployment script or a support session.
- The wizard names the specific reason on failure (rejected credentials, timeout, refused) by reading the relay's own report instead of a bare "failed". Localized in all 10 wizard languages.
- Removed `installer/test-connection.ps1` and the messages only it used: a wire-level handshake proved reachability but not credentials, which is the case that actually fails. The endpoint pages keep their "Test connection" button, now backed by the real client.

## 0.3.1 — 2026-10-14

- O relay agora implementa o protocolo de serviço do Windows: o gerenciador de
  serviços o inicia, para e reinicia em falha como qualquer outro serviço. Antes o
  instalador registrava um executável de console e toda tentativa de início
  falhava com o erro 1053 — o serviço nunca subia. Nas demais plataformas nada
  mudou.
- Instalação silenciosa parametrizável: `/CONFIG=<arquivo>` usa uma configuração
  que já existe, `/SERVICE=0|1` decide se o serviço é registrado e
  `/SERVICESTART=auto|delayed-auto|demand` define o tipo de inicialização.
- Falhas depois da cópia dos arquivos devolvem código de saída **9** e ficam
  registradas em `{app}\install-report.txt`, uma linha por etapa; nada é exibido
  em modo silencioso. O Inno Setup devolve 0 para falha de script, então esse
  código é o único jeito de falhar alto.
- O instalador confirma que o relay subiu, não apenas que o serviço aceitou o
  pedido, e exige um `status.json` escrito depois de a verificação começar.
- Corrigido: o `sc.exe` não aceitava o caminho padrão de instalação (erro 1639) e
  o estado do serviço era lido de um texto que muda com o idioma (`ESTADO`), o que
  fazia um serviço rodando parecer `unknown`.
- Corrigido: no modo silencioso a configuração gerada saía NATS→NATS em vez de
  Redis→Redis, um upgrade silencioso pulava o serviço, e o motivo pelo qual o
  relay rejeitou uma configuração se perdia.

## 0.3.0 — 2026-10-14

- Seleção de backend neutra de protocolo: `input` e cada saída usam exatamente um
  de `redis` ou `nats`.
- Backend Core NATS de entrada e saída (Pub/Sub apenas, não JetStream): `servers`
  com regra de uma família de transporte por lista (`nats://`, `tls://`, `ws://`,
  `wss://`), `name`/`connect_timeout_ms`, `ignore_discovered_servers` opcional e
  assinaturas `nats_subject` com wildcards nativos `*`/`>`.
- Variantes de autenticação NATS: `none` (padrão), `user_password`, `token`,
  `nkey` e `credentials_file`, com TLS/mTLS sob `tls`. Segredos sempre por variável
  de ambiente ou caminho de arquivo, nunca inline no JSON.
- Loop guard NATS→NATS via header `ConflatedPS-Hop`, com contador
  `loop_suppressed_messages_total`. Headers de aplicação não são encaminhados.
- Política de mensagem grande aplicada ao `max_payload` anunciado pelo servidor
  NATS (menos os bytes do header de hop), com preflight de cada chunk.
- Ciclo de vida de conexão NATS no status (`nats_client_connects_total`,
  `nats_client_disconnects_total`, `nats_client_slow_consumers_total`) e falha
  rápida quando um segredo referenciado está ausente.

## 0.2.2 — 2026-10-08

- A entrada Pub/Sub envia `CLIENT SETNAME` antes de assinar (TCP simples), então
  `CLIENT LIST` mostra o nome `-i` também no servidor de entrada. Entradas TLS
  continuam sem nome.

## 0.2.1 — 2026-10-08

- `client_name` `{host}` cai para o hostname do SO quando
  `COMPUTERNAME`/`HOSTNAME` não estão definidos, resolvendo a máquina em vez de
  `unknown` sob systemd.
- `deduplication.ttl_ms` documentado como TTL de diferença.

## 0.2.0 — 2026-10-08

- Padrão de `deduplication.ttl_ms` mudou de 5000 para **0**: deduplicação só é
  ligada explicitamente por saída, perfil ou política de canal.
- Filtros ordenados de entrada/saída com seletores `glob`, `regex` e literal.
- Caches LRU limitadas e independentes para filtros de entrada, filtros de cada
  saída e políticas de canal; o endpoint opt-in `GET /filters` expõe as entradas.
- Buckets de canais conflados agrupados por intervalo, cada timer descarregando
  só seu próprio bucket; passthrough direto com janela em voo limitada.
- Métricas de espera na fila e RTT de publish por saída.
- Benchmarks de hot path e de política mista migrados para Rust.
- Imagem única com Redis/Valkey selecionável por `KV_ENGINE`.
- Proxies e drivers E2E migrados para o binário Rust `redis-conflated-e2e`.

## 0.1.4 — 2026-10-06

- Reload de configuração por `SIGHUP` no Unix. O arquivo novo é validado antes da
  troca; configuração inválida mantém o runtime atual. Mudanças de logging e de
  lock ainda exigem reinício completo.

## 0.1.3 — 2026-10-06

- Perfis por saída com sobrescritas parciais de conflation/TTL e políticas de canal
  ordenadas com regra `default` final obrigatória.
- Grupos TTL nomeados com expiração compartilhada arredondada por relógio e caches
  limitados. Durações positivas limitadas a 365 dias.

## 0.1.2 — 2026-10-06

- Supressão de duplicatas por saída com `deduplication.ttl_ms` (na época padrão
  5000 ms), com cache só em memória.
- Contadores globais e por saída de deduplicação e suporte nos templates Zabbix.

## 0.1.1 — 2026-10-06

- Alvos de tamanho de requisição RESP e divisão de descargas confladas por bytes
  exatos e contagem de comandos.
- Políticas de mensagem grande: `send`, `truncate` e `drop`.
- Exclusões explícitas de eco de saída e de Pub/Sub do Sentinel.
- Falhas não são retentadas: o serviço conta mensagens falhas/incertas, limita os
  logs de erro e continua.

## 0.1.0 — 2026-10-05

- Primeira versão: relay de Pub/Sub Redis com conflation e mapeamento de canais,
  conflation transacional e builds multiplataforma.
