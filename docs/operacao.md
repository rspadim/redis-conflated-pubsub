# Operação e monitoramento

## systemd (Linux)

Instale o template de unit e habilite o serviço:

```sh
sudo install -m 0644 redis-conflated-pubsub.service.example /etc/systemd/system/redis-conflated-pubsub.service
sudo systemctl daemon-reload
sudo systemctl enable --now redis-conflated-pubsub.service
sudo systemctl status redis-conflated-pubsub.service
```

O arquivo `redis-conflated-pubsub.service.example` está na raiz do repositório;
ajuste `User`, `ExecStart` e os caminhos antes de instalar em produção.

## Recarregar configuração no Unix

Envie `SIGHUP` para recarregar o JSON configurado sem reiniciar a unit:

```sh
sudo systemctl kill --signal=HUP --kill-whom=main redis-conflated-pubsub.service
```

O serviço valida o arquivo **antes** de trocar; uma configuração inválida é
rejeitada e o runtime atual continua rodando. Um reload válido drena as filas
pendentes e reinicia os workers de entrada/saída, então há uma breve lacuna na
entrada Pub/Sub e mensagens podem ser perdidas (Pub/Sub não tem replay). Os
contadores de status e o uptime são zerados.

Um `instance_lock.path` alterado é adquirido durante o reload **antes** de liberar
o antigo, e o reload é rejeitado se o novo lock estiver indisponível. Mudanças em
`logging` ou a troca do próprio executável ainda exigem reinício completo.

## Status e métricas

Habilite o endpoint HTTP de status:

```json
"status": {
  "http": {
    "bind": "127.0.0.1",
    "port": 9090
  }
}
```

`GET /` serve as métricas por saída. `GET /filters` expõe os nomes de canal
completos e as entradas atuais dos caches de filtro/política; fica **desligado por
padrão** e só deve ser habilitado com
`status.http.filters_endpoint_enabled: true` em uma interface confiável/privada,
porque nomes de canal podem ser sensíveis.

```sh
curl http://127.0.0.1:9090/
```

Alternativamente, `status.path` grava um snapshot JSON atômico em arquivo, útil
quando não se quer expor uma porta.

## Monitoramento Zabbix

O coletor Python 3 lê o status por HTTP e envia valores trapper com
`zabbix_sender`, usando apenas a biblioteca padrão.

### Pré-requisitos

- Serviço rodando com o listener HTTP de status habilitado.
- Python 3 e `zabbix_sender` na máquina do coletor.
- Acesso de rede do coletor ao status e ao servidor/proxy Zabbix (porta padrão
  `10051`).

### Configurar e testar

```sh
cp monitoring/config.example.json monitoring/config.json
curl --fail http://127.0.0.1:9090/
python3 monitoring/zabbix_sender.py --config monitoring/config.json
```

Defina `base_url` para a raiz do endpoint de status, `zabbix_server`/`zabbix_port`
para o servidor ou proxy, e `host` para o **nome técnico exato** do host no
Zabbix. `key_prefix` precisa casar com o template importado (padrão
`redis_conflated_pubsub.`). Use `sender_path` com caminho absoluto se o
`zabbix_sender` não estiver no `PATH`.

Com uma resposta válida, o coletor envia **34 valores** em um lote. Se o endpoint
estiver indisponível ou a resposta for inválida, ele envia apenas `health=0` e
deixa os outros valores como estão.

Se o host já tem um agente Zabbix, `zabbix_config` permite herdar `ServerActive`,
`Hostname` e TLS do arquivo do agente (normalmente
`/etc/zabbix/zabbix_agentd.conf`). `host` continua obrigatório e deve ser o
`Hostname` do agente.

### Importar o template

Importe **um** XML conforme a versão do servidor:

| Versão Zabbix | Arquivo | Menu |
| --- | --- | --- |
| 5.0 | `monitoring/zabbix_template_5.0.xml` | Configuration → Templates → Import |
| 7.0 | `monitoring/zabbix_template_7.0.xml` | Data collection → Templates → Import |

Crie ou selecione um host, vincule **Template Redis Conflated Pub/Sub** e defina o
`host` do coletor para o nome técnico do host. Os itens são trapper: não é preciso
agente Zabbix para o coletor rodar. Confira `health = 1` e `schema_version = 5`.

### Valores coletados

| Campo do status | Sufixo da chave | Significado |
| --- | --- | --- |
| derivado de `state` | `health` | `1` quando `state` é `running`, senão `0` |
| `schema_version` | `schema_version` | Inteiro (permanece `5`) |
| `state` | `state` | Estado do serviço |
| `updated_at`, `started_at`, `last_input_at`, `last_flush_at` | mesmo nome | Timestamps; nulo vira `never` |
| `last_error` | `last_error` | Texto; nulo vira `none` |
| `uptime_seconds` | `uptime_seconds` | Segundos |
| `input_messages_total`, `input_payload_bytes_total` | mesmo nome | Contagem e bytes de entrada |
| `output_batches_total`, `output_messages_total`, `output_payload_bytes_total` | mesmo nome | Lotes, mensagens e bytes publicados |
| `conflated_messages_total`, `conflated_payload_bytes_total` | mesmo nome | Mensagens superadas e bytes |
| `deduplicated_messages_total`, `deduplicated_payload_bytes_total` | mesmo nome | Suprimidos por TTL |
| `excluded_messages_total` | `excluded_messages_total` | Mensagens excluídas |
| `dropped_messages_total`, `dropped_payload_bytes_total` | mesmo nome | Descartes por política e falhas pré-envio definitivas |
| `truncated_messages_total`, `truncated_payload_bytes_total` | mesmo nome | Mensagens truncadas e bytes removidos |
| `publish_errors_total`, `publish_error_messages_total` | mesmo nome | Falhas de publish e mensagens afetadas |
| `uncertain_transactions_total`, `uncertain_messages_total` | mesmo nome | Transações e mensagens incertas |
| `input_reconnects_total`, `output_reconnects_total` | mesmo nome | Reconexões Redis |
| `nats_client_connects_total`, `nats_client_disconnects_total` | mesmo nome | Conexões NATS (o Core NATS reconecta internamente) |
| `loop_suppressed_messages_total` | `loop_suppressed_messages_total` | Descartes do loop guard NATS |
| `pending_keys` | `pending_keys` | Pendências |
| `outputs` | `outputs_json` | JSON compacto com as métricas por saída |

Os contadores de deduplicação são aditivos; `schema_version` permanece `5`. O JSON
por saída inclui métricas de publicação, conflation e deduplicação por destino,
com a mesma semântica de bytes de payload cru do contador global. O texto de erro
é limitado a 2.048 caracteres e quebras de linha são normalizadas.

### Rodar em agenda

Por exemplo, uma vez por minuto com cron:

```cron
* * * * * /usr/bin/python3 /opt/redis-conflated-pubsub/monitoring/zabbix_sender.py --config /etc/redis-conflated-pubsub/monitoring.json >> /var/log/redis-conflated-pubsub-monitoring.log 2>&1
```

Use caminhos absolutos: o cron tem `PATH` e diretório de trabalho limitados. Cada
execução envia o snapshot de status mais recente. O agente Zabbix não roda o
coletor; o `zabbix_sender` conecta direto ao servidor ou proxy.

## Solução de problemas

- **Nada chega nas saídas** — confirme o casamento de assinatura e o mapeamento de
  canal; em Redis, o `PSUBSCRIBE` usa glob e distingue maiúsculas.
- **Publish falhando com `0` assinantes em NATS** — isso é esperado no Core NATS.
  Leia `publish_errors_total` e `uncertain_messages_total`.
- **Mensagens somem em picos** — verifique `shed_messages_total`,
  `pending_queue_bytes` e `oldest_pending_age_ms`. A fila padrão é ilimitada, então
  descarte só ocorre com limites configurados.
- **`--check-config` reprova** — a mensagem indica o caminho do campo; valide
  `default` por último em `channel_policies`, exatamente um backend por lado e
  arquivos mTLS pareados.
