# Backend NATS

A entrada e **cada** saída selecionam exatamente um backend. Basta adicionar um
objeto `nats` ao lado do `redis` existente: as chaves são aditivas, então
configurações Redis continuam válidas sem alteração.

- Ou `redis` ou `nats` sozinho é válido.
- Exatamente um backend por lado é obrigatório. Omitir os dois **ou** definir os
  dois falha no `--check-config` e na inicialização:

  ```text
  input must select exactly one backend, but both `redis` and `nats` are configured
  input must select one backend with `redis` or `nats`
  ```

- O JSON Schema gerado codifica a mesma regra com `oneOf` sobre
  `required: ["redis"]` / `required: ["nats"]` para `input` e para cada saída.

!!! info "Somente Core NATS Pub/Sub"
    Não é JetStream: sem persistência e sem replay.

## Campos do endpoint

| Campo | Significado |
| --- | --- |
| `servers` | Uma ou mais URLs de seed. Obrigatório e não vazio. O esquema é obrigatório e seleciona o transporte: `nats://` (TCP), `tls://` (TLS), `ws://`/`wss://` (WebSocket). Um `host:port` puro é rejeitado. |
| `name` | Nome de conexão opcional, reportado na listagem de clientes do servidor NATS. |
| `connect_timeout_ms` | Timeout de conexão e handshake. Padrão `5000`. |
| `ignore_discovered_servers` | Quando `true`, só os `servers` configurados são usados e servidores anunciados pelo cluster são ignorados. Útil atrás de proxy ou ingress WebSocket com endereços internos inalcançáveis. |
| `auth` | Variante de autenticação. Padrão `none`. |
| `tls` | Raiz confiável e/ou identidade do cliente. Veja abaixo. |

### Uma família de transporte por lista

Toda URL de uma mesma lista `servers` precisa usar a mesma família de transporte,
e `ws`/`wss` não podem ser misturados com `nats`/`tls` na mesma lista (uma conexão
NATS exige uma única família). Entradas e saídas diferentes podem usar famílias
diferentes. Uma lista mista é rejeitada:

```sh
input.servers mixes nats and wss transports (Tcp and WebSocketTls); one NATS connection requires a single transport family
```

## Autenticação

Segredos são sempre referenciados por **nome de variável de ambiente** ou
**caminho de arquivo**, nunca escritos no JSON.

| `auth.type` | Referência de segredo | Observações |
| --- | --- | --- |
| omitido ou `none` | nenhuma | Apenas desenvolvimento; use uma rede protegida. |
| `user_password` | `username` + `password_env` | Usuário mais a variável de ambiente com a senha. |
| `token` | `token_env` | Variável de ambiente com o token. |
| `nkey` | `seed_file` | Caminho do arquivo de seed NKey. |
| `credentials_file` | `path` | Caminho do arquivo `.creds` (**JWT de usuário + sua seed NKey**), não um JWT bearer. |

=== "user_password"

    ```json
    "auth": { "type": "user_password", "username": "relay", "password_env": "NATS_PASSWORD" }
    ```

=== "token"

    ```json
    "auth": { "type": "token", "token_env": "NATS_TOKEN" }
    ```

=== "nkey"

    ```json
    "auth": { "type": "nkey", "seed_file": "/run/secrets/relay.nk" }
    ```

=== "credentials_file"

    ```json
    "auth": { "type": "credentials_file", "path": "/run/secrets/relay.creds" }
    ```

TLS é separado de `auth` e é selecionado pelo esquema da URL; o bloco `tls` apenas
o estende com raízes confiáveis ou identidade do cliente. Para conexões remotas
com senha ou token, use `tls://` ou `wss://` para que o segredo não trafegue em
claro.

```json
"tls": {
  "root_cert_file": "/run/secrets/nats-ca.pem",
  "client_cert_file": "/run/secrets/relay-cert.pem",
  "client_key_file": "/run/secrets/relay-key.pem"
}
```

mTLS exige `client_cert_file` e `client_key_file` juntos (o `--check-config`
rejeita um sem o outro).

!!! danger "Nunca embuta segredos no JSON"
    Senha, token, seed NKey ou material de chave nunca vão inline. Referencie por
    variável de ambiente (`password_env`, `token_env`) ou caminho de arquivo
    (`seed_file`, `path` e os arquivos de `tls`), e injete o valor no deploy.
    Mantenha segredos fora do Git, dos logs e dos argumentos de processo.

## Subjects e wildcards

Assinaturas de entrada NATS usam o tipo nativo `nats_subject` e a semântica NATS.
Globs de `SUBSCRIBE`/`PSUBSCRIBE` do Redis **não** são traduzidos.

- Entrada Redis: `{"type": "subscribe", "channel": "prices:EURUSD"}` ou
  `{"type": "psubscribe", "pattern": "prices:*"}` com semântica glob do Redis.
- Entrada NATS: `{"type": "nats_subject", "subject": "prices.EURUSD"}` ou
  `{"type": "nats_subject", "subject": "prices.>"}`.

Em um subject NATS, `.` separa tokens, `*` casa exatamente um token e `>` casa um
ou mais tokens finais, e precisa ser o último. `prices:*` (Redis) e `prices.>`
(NATS) são padrões diferentes; o relay nunca converte um no outro. O
`--check-config` rejeita subjects vazios, com espaços, com tokens vazios, com
wildcard parcial (como `pric*`) ou com um `>` que não seja final.

Para saídas NATS, o mapeamento mantém o significado usual: `output_prefix`/
`output_suffix` da assinatura, depois `channel_prefix`/`channel_suffix` da saída,
concatenados em volta do subject de origem. A string final é usada **diretamente**
como subject de publicação. Não há renomeação nem escape automático, então o
resultado mapeado precisa ser um subject concreto e válido (sem `*` ou `>`); um
subject mapeado inválido é rejeitado com erro definitivo antes do envio, em vez de
ser publicado.

### Sem ack e sem contagem de assinantes

Core NATS **não tem ack por mensagem nem contagem de assinantes**. Um publish
bem-sucedido significa que o comando foi aceito no buffer local do cliente; o
`flush` que segue cada lote drena o **buffer local de escrita** para o socket, e o
`flush` do `async-nats` **não** espera o servidor processar os comandos.

Portanto, a saída NATS reporta `0` assinantes — não leia isso como "ninguém está
escutando". Leia `publish_errors_total`, `uncertain_messages_total`,
`input_reconnects_total` e `output_reconnects_total`.

## Loop guard

Uma saída publicada em uma conexão pode casar com a própria assinatura de entrada
em outra, então ciclos NATS→NATS são quebrados com o header `ConflatedPS-Hop: 1`
(namespaceado para não colidir com headers da aplicação). Toda saída NATS carimba
esse header em cada mensagem encaminhada, e a entrada NATS descarta qualquer
mensagem que já o carregue.

- O payload **nunca** é modificado: o guard é invisível para assinantes que
  ignoram headers.
- **Apenas o payload é encaminhado, não os headers da aplicação.** A saída define
  só o `ConflatedPS-Hop`, então headers de entrada (content-type, tracing,
  `Nats-Msg-Id`, …) são descartados quando a mensagem cruza o relay. Isso é
  intencional: propagá-los tornaria o tamanho na rede variável e complicaria a
  contabilização do teto de payload.
- Mensagens suprimidas são contadas em `loop_suppressed_messages_total`.
- O guard é específico de NATS: uma entrada Redis não tem headers para carimbar ou
  ler e mantém o tratamento de eco no mesmo servidor. Uma entrada Redis nunca
  recebe tráfego de saída NATS.

## Exemplo: NATS → NATS

Encaminha `market.>` de um cluster NATS para um endpoint de arquivo, mapeando
`market.EURUSD` para `archive.market.EURUSD.v1`:

```json
{
  "max_bytes_per_exec": 4194304,
  "oversized_message_policy": "send",
  "client_name": "ConflatedPS-{version}-{role}::{user}::{host}",
  "input": {
    "nats": {
      "servers": ["nats://nats-1:4222", "nats://nats-2:4222"],
      "name": "conflated-input",
      "connect_timeout_ms": 5000,
      "ignore_discovered_servers": false,
      "auth": {
        "type": "user_password",
        "username": "relay",
        "password_env": "NATS_INPUT_PASSWORD"
      }
    },
    "subscriptions": [
      {
        "type": "nats_subject",
        "subject": "market.>",
        "output_prefix": "",
        "output_suffix": ""
      }
    ]
  },
  "outputs": {
    "nats-archive": {
      "nats": {
        "servers": ["tls://nats.example.net:4222"],
        "name": "conflated-archive",
        "auth": {
          "type": "credentials_file",
          "path": "/run/secrets/relay.creds"
        },
        "tls": {
          "root_cert_file": "/run/secrets/nats-ca.pem"
        }
      },
      "channel_prefix": "archive.",
      "channel_suffix": ".v1",
      "conflation": {
        "interval_ms": 250,
        "max_commands_per_exec": 256
      },
      "deduplication": {
        "ttl_ms": 5000
      }
    }
  },
  "instance_lock": {
    "path": "redis-conflated-pubsub.lock"
  },
  "status": {
    "http": {
      "bind": "127.0.0.1",
      "port": 9090
    }
  },
  "logging": {
    "directory": "logs",
    "prefix": "redis-conflated-pubsub",
    "enabled": true,
    "level": "info",
    "retention_days": 14,
    "max_total_size_mb": 1024
  }
}
```

## Exemplo: Redis → NATS

Encaminha `feed:*` de uma entrada Redis para NATS via WebSocket TLS, mapeando
`feed:alpha` para `nats.feed:alpha`, com a variante `token`:

```json
{
  "input": {
    "redis": {
      "host": "redis.example.net",
      "port": 6379,
      "username": null,
      "password_env": "REDIS_PASSWORD",
      "tls": false,
      "database": 0,
      "connect_timeout_ms": 5000
    },
    "subscriptions": [
      {
        "type": "psubscribe",
        "pattern": "feed:*",
        "output_prefix": "",
        "output_suffix": ""
      }
    ]
  },
  "outputs": {
    "nats-out": {
      "nats": {
        "servers": ["wss://nats.example.net:443"],
        "ignore_discovered_servers": true,
        "auth": {
          "type": "token",
          "token_env": "NATS_TOKEN"
        }
      },
      "channel_prefix": "nats.",
      "channel_suffix": "",
      "conflation": {
        "interval_ms": 100
      }
    }
  },
  "instance_lock": {
    "path": "redis-conflated-pubsub.lock"
  }
}
```

!!! tip "Exemplo completo"
    O repositório traz
    [`config.nats.example.json`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/config.nats.example.json)
    com entrada NATS via `credentials_file`, saída NATS com token e uma saída de
    cópia em Redis. Valide antes de rodar:

    ```sh
    ./redis-conflated-pubsub --check-config --config config.nats.example.json
    ```
