# Configuração

O relay lê um único JSON. Os campos abaixo são os que mais afetam o
comportamento; a referência exata e os limites são validados pelo
`--check-config` e descritos pelo JSON Schema gerado
(`--config-json-schema`).

## Perfis e políticas de canal

Os padrões por saída ficam em `conflation.interval_ms` e
`deduplication.ttl_ms`.

- Intervalos e TTLs positivos são limitados a **365 dias**.
- Intervalos **não positivos** selecionam publicação direta.
- TTLs **não positivos** desativam a deduplicação.

`deduplication_groups` da própria saída mapeia nomes para
`{ "ttl_ms", "round_ms", "restart_on_change", "max_members", "max_cache_bytes" }`.

| Campo | Efeito |
| --- | --- |
| `round_ms` | Arredonda para baixo o timestamp Unix usado no TTL do grupo, formando um bucket. `0` desativa. O arredondamento pode encurtar um TTL ativo em até `round_ms`. |
| `restart_on_change` | Reancora o prazo depois de um valor alterado ou de um novo membro publicado. `false` mantém a expiração fixa. |
| `max_members` / `max_cache_bytes` | Limites do grupo (padrão 16.384 canais e 64 MiB; máximo 100.000 membros e 256 MiB). Ao encher, remove o membro menos usado recentemente. |

Para TTLs positivos, `round_ms` não pode exceder `ttl_ms` (checado pelo
`--check-config`). Suprimidos duplicados e valores intermediários consumidos pela
conflation **não** renovam o prazo. Um membro individual maior que o limite de
bytes ainda é publicado, mas não é cacheado, com um aviso.

### TTL de diferença

`deduplication.ttl_ms` é um **TTL de diferença**: apenas payloads idênticos
**consecutivos** (mesmo canal mapeado e mesmos bytes) são suprimidos, e só até a
janela expirar.

- Um payload alterado sempre publica (sujeito à conflation) e substitui o valor
  lembrado — logo `A→B→A` publica os três.
- Depois da janela, o valor atual é enviado de novo mesmo sem mudança, agindo
  como refresh.

`deduplication.in_flight_suppression` (padrão `false`) trata um valor enviado mas
ainda não confirmado como já publicado para fins de supressão: repetições
idênticas são descartadas sem esperar o lote em voo. Uma falha definitiva antes do
envio desfaz o valor lembrado; um resultado ambíguo o mantém.

`max_entries` e `max_cache_bytes` opcionais limitam o cache TTL por canal com
evicção LRU (sem valor definido = ilimitado) e contam evicções em
`deduplication_evictions_total`.

### Filas de entrada

Cada saída pode limitar sua fila com `queue_max_messages` e `queue_max_bytes`
(sem valor = ilimitado) e `queue_overflow_policy`:

| Política | Comportamento |
| --- | --- |
| `drop_newest` | Padrão. Descarta a mensagem que chega. |
| `drop_oldest` | Descarta a mais antiga da fila. |
| `drop_by_age` | Descarta conforme `queue_max_age_ms`. |

Mensagens descartadas são contadas por saída e globalmente em
`shed_messages_total`/`shed_payload_bytes_total`. O gauge de bytes da fila é
`pending_queue_bytes` e a idade do pendente mais antigo é `oldest_pending_age_ms`.

!!! warning "Pub/Sub não tem replay"
    Qualquer política de descarte joga eventos fora de forma explícita. A
    configuração padrão permanece ilimitada.

### Perfis

`profiles` são sobrescritas parciais: defina `conflation.interval_ms`,
`deduplication.ttl_ms` ou `deduplication.group` conforme necessário; o que não for
definido herda os padrões da saída. Um perfil **não pode** definir
`deduplication.ttl_ms` e `deduplication.group` ao mesmo tempo.

```json
"profiles": {
  "fast_no_dedup": {
    "conflation.interval_ms": 100,
    "deduplication.ttl_ms": 0
  },
  "shared_symbols": {
    "conflation.interval_ms": 250,
    "deduplication.group": "symbols"
  }
}
```

### Políticas de canal

Cada entrada ordenada de `channel_policies` tem **exatamente um** seletor: `glob`,
`prefix`, `suffix` ou `"default": true` (coloque o default por último). Ela
seleciona um perfil **ou** sobrescritas inline usando chaves pontuadas literais.

As políticas casam com o **canal final mapeado da saída**, depois dos prefixos e
sufixos da assinatura e da saída. Apenas a **primeira** política que casa é
selecionada para cada canal. O `--check-config` exige que a regra default seja a
última.

```json
"channel_policies": [
  { "glob": "db1:sub:test-feed:alpha:source", "profile": "fast_no_dedup" },
  { "prefix": "db1:sub:test-feed:beta:", "profile": "shared_symbols" },
  {
    "default": true,
    "conflation.interval_ms": 250,
    "deduplication.group": "symbols"
  }
]
```

Sem grupo, os TTLs de deduplicação expiram por canal; membros de um grupo
compartilham o prazo. Cada saída usa uma cadência de agendador por intervalo
positivo distinto; intervalos iguais compartilham a cadência.

Para testar carga de políticas localmente,
[`config.channel-policies-load.example.json`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/config.channel-policies-load.example.json)
traz 16 regras de tenant ordenadas e um default final.

## Filtros de canal

`input.filters` avalia o canal informado pelo Redis depois que um
`SUBSCRIBE`/`PSUBSCRIBE` casa, mas **antes** do `output_prefix`/`output_suffix`
da assinatura. Cada saída pode ter seus próprios `filters`, avaliados no canal
mapeado pela assinatura (após prefixo/sufixo de entrada, antes do
`channel_prefix`/`channel_suffix` da saída).

As regras são ordenadas: **todas** são avaliadas e a `action` (`accept` ou `deny`)
da **última que casar** decide. Regras que não casam não fazem nada. Se nenhuma
casar, `filter_default` decide (padrão `accept`).

Cada regra tem exatamente um seletor:

| Seletor | Semântica |
| --- | --- |
| `glob` | Suporta `*`, `?`, classes de caractere e escape com barra invertida. |
| `regex` | Sintaxe Rust; busca em qualquer posição. Use `^`/`$` para o canal inteiro. |
| `raw`, `single`, `string` | Aliases de igualdade exata. `*` é literal. |

```json
{
  "filters": [
    {"glob": "events:*", "action": "deny"},
    {"regex": "^events:public:[0-9]+$", "action": "accept"},
    {"raw": "events:public:42", "action": "deny"}
  ],
  "filter_default": "accept"
}
```

No exemplo, a regra literal final sobrescreve a regex anterior para aquele canal
exato.

### Caches

Os seletores são compilados uma vez em variantes internas e as strings originais
são descartadas após o setup. As capacidades são independentes:

- `input.filter_cache_max_entries`
- `outputs.<name>.filter_cache_max_entries`
- `outputs.<name>.channel_policy_cache_max_entries`

Cada uma tem padrão **16.384**, aceita **0 para desativar** aquele cache e é
limitada a **100.000**. O limite vale por cache; a memória total cresce com o
número de filtros e saídas. Na evicção, só sai o canal menos usado recentemente.
Uma recarga aceita recria os workers e seus caches.

## Conflation e tamanho de lote

Intervalos positivos mantêm a última mensagem por canal mapeado e descarregam em
transações `MULTI`/`EXEC`.

| Campo | Significado |
| --- | --- |
| `max_bytes_per_exec` | Alvo de bytes por requisição. |
| `max_commands_per_exec` | Limite de comandos por transação. |
| `oversized_message_policy` | `send` (padrão), `truncate` ou `drop`. |

## Nome do cliente

`client_name` aparece em `CLIENT LIST`. O template padrão é
`ConflatedPS-{version}-{role}::{user}::{host}`:

- `{version}` — versão do programa.
- `{role}` — `i` para a entrada ou `o-<output>` para cada saída.
- `{user}` — `USERDOMAIN\USERNAME` no Windows ou `USER`/`LOGNAME` nos demais.
- `{host}` — `COMPUTERNAME`/`HOSTNAME`, com fallback para o hostname do SO.

Valores ausentes caem para `unknown`. Um template sem `{role}` recebe
`-<role>` acrescentado, e um `client_name` vazio desativa o nome.

Conexões de saída enviam `CLIENT SETNAME` a cada reconexão. A entrada Pub/Sub faz
o mesmo em TCP simples: autentica e nomeia o socket **antes** de assinar, então o
nome da entrada exige uma entrada sem TLS. Toda conexão também envia
`CLIENT SETINFO` (`lib-name`/`lib-ver`) quando o servidor é Redis 7.2 ou mais novo.

## Logging

```json
"logging": {
  "directory": "logs",
  "prefix": "redis-conflated-pubsub",
  "enabled": true,
  "level": "info",
  "retention_days": 14,
  "max_total_size_mb": 1024
}
```

`logging.prefix` nomeia os arquivos (`{prefix}.{YYYY-MM-DD}[.{n}]`) e delimita a
limpeza por retenção, permitindo que vários serviços compartilhem um diretório sem
tocar nos arquivos uns dos outros. `logging.enabled: false` desliga o log em
arquivo por completo; o serviço continua rodando e o `--check-config` ainda valida
os demais campos.

## Escopo

JetStream (persistência, ack, replay durável) e Redis Streams estão **fora de
escopo** de forma permanente. O relay é um conflator de Pub/Sub efêmero; adicionar
um log durável mudaria seu contrato.
