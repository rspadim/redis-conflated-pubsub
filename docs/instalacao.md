# Instalação

Há quatro caminhos. Este página cobre o instalador do Windows e os pacotes Linux;
para o binário direto veja [Início rápido](inicio-rapido.md) e para contêiner veja
[Docker e testes](docker.md).

## Windows — instalador

Baixe `ConflatedPubSubSetup-<versão>.exe` na
[página de releases](https://github.com/rspadim/redis-conflated-pubsub/releases).
Requer Windows 10 ou mais novo e privilégios de administrador, porque registra um
serviço.

O wizard abre com um menu de seis opções:

- configurar entrada Redis → saída Redis (padrão)
- entrada NATS → saída NATS
- entrada Redis → saída NATS
- entrada NATS → saída Redis
- usar um `config.json` que já existe
- instalar somente o executável, sem configurar

As perguntas seguem a escolha: páginas de entrada e de saída (endpoint, banco, nome
da variável de ambiente da senha, padrão de inscrição ou subject, prefixo e sufixo
do canal) e uma de ajustes (intervalo de conflação, TTL de deduplicação, porta HTTP
do status, nível de log). O prefixo da saída já vem com `conflated:` porque o relay
recusa, com entrada e saída no mesmo servidor Redis, uma configuração sem prefixo ou
sufixo: a própria saída casaria com a assinatura de entrada.

Ao sair da última página, o wizard monta o arquivo e o valida com o
`--check-config` do próprio relay, num rascunho no diretório temporário — antes de
escrever qualquer coisa em `Program Files`. Se já existir um arquivo no destino, ele
é mantido e validado como está; se não existir, é gerado a partir das respostas. Em
seguida o instalador copia os arquivos, grava o `config.json` com caminhos
absolutos (um serviço do Windows inicia com o diretório de trabalho em `System32`),
registra o serviço com reinício automático em falha e, no fim, confirma se o relay
**realmente subiu** — `sc start` sozinho não provaria isso.

| Onde | O quê |
| --- | --- |
| `C:\Program Files\Conflated Pub/Sub\` | executável, `config.json`, os exemplos e `scripts\` |
| `logs\` e `status.json` | dentro dessa pasta, por caminhos absolutos no `config.json` |
| Menu Iniciar | validar a configuração, editar `config.json`, documentação, desinstalar |

Instalação silenciosa, para deploy automatizado:

```powershell
.\ConflatedPubSubSetup-<versão>.exe /VERYSILENT /CONFIG=C:\relay\config.json /SERVICE=1 /SERVICESTART=auto
```

| Parâmetro | Significado |
| --- | --- |
| `/CONFIG=<arquivo>` | Usa uma configuração que já existe. Ela precisa existir: sem isso o instalador se recusa a começar, e nada é gerado. |
| `/SERVICE=0\|1` | `1` (padrão no modo silencioso) registra e inicia o serviço; `0` apenas copia os arquivos, para quem vai terminar à mão. No wizard quem decide são as tarefas — e atenção: o Inno deixa uma tarefa `checkedonce` desmarcada numa instalação posterior. |
| `/SERVICESTART=auto\|delayed-auto\|demand` | Tipo de inicialização; `auto` é o padrão no modo silencioso. No wizard é `auto`, a menos que a tarefa "iniciar automaticamente" esteja desmarcada. |

Sem `/CONFIG` a configuração é gerada com as respostas padrão do wizard (entrada
Redis para saída Redis); para mudá-las, rode o wizard ou edite o `config.json`
depois.

Um valor inválido aborta antes de escrever qualquer coisa, e **nada é exibido** em
modo silencioso — uma caixa de diálogo esperaria por um clique que nunca vem.
`/LOG=<arquivo>` guarda o log do próprio Inno.

O relay implementa o protocolo de serviço do Windows, então o gerenciador de
serviços consegue iniciá-lo, pará-lo e reiniciá-lo em falha. O instalador confirma
que o relay subiu, não apenas que o serviço aceitou o pedido de início.

### O que um deploy automatizado consegue checar

| Código de saída | Significado |
| --- | --- |
| `0` | Tudo o que o instalador foi chamado a fazer foi feito. |
| `1` | Os parâmetros foram rejeitados, ou o `/CONFIG` não existe: nada foi escrito. |
| `9` | Uma etapa **depois** da cópia dos arquivos falhou. O Inno devolve `0` para falha de script (verificado: `RaiseException` e `Abort()` em `ssPostInstall` deixam o código em `0`), então o instalador reporta essa pelo `GetCustomSetupExitCode`. |
| `5`, `7`, `8` | Códigos do próprio Inno: cancelado, não foi possível prosseguir, precisa reiniciar. |

O `{app}\install-report.txt` diz qual etapa falhou, uma linha por etapa:

```
configuration: C:\Program Files\Conflated Pub/Sub\config.json
configuration: generated
configuration: accepted by the relay
service: redis-conflated-pubsub (auto)
service: installed
service: started
relay: running
result: ok
```

Uma desinstalação silenciosa mantém o `config.json`: não há ninguém para responder,
e apagá-lo é a resposta destrutiva.

### Credenciais

O relay lê senha de variável de ambiente, e um serviço do Windows **não** enxerga
variáveis de usuário: uma variável criada no seu perfil é invisível para o serviço,
que então sai na inicialização. O wizard resolve isso — o campo de senha grava o
valor no **ambiente do próprio serviço**, uma entrada `Environment` na chave do
serviço que o gerenciador de serviços injeta apenas naquele processo, legível só por
administradores. Deixar o campo vazio é suportado, mas o wizard avisa que a variável
precisa existir nesse ambiente.

### Conta de serviço

O serviço roda como a conta virtual `NT SERVICE\redis-conflated-pubsub`, sem senha,
em vez de `LocalSystem`: ele precisa da rede e do próprio diretório, nada mais. O
instalador concede escrita apenas na pasta de instalação. Se a máquina recusar a
conta virtual, ele volta para `LocalSystem` e informa isso na tela.

### Desinstalar

O desinstalador para e remove o serviço e pergunta se mantém o `config.json` e os
logs.

## Linux — pacotes

Cada release publica deb, rpm, apk e Arch, construídos a partir do binário **musl
estático**, então não há dependência de runtime:

```sh
# Debian/Ubuntu
sudo dpkg -i redis-conflated-pubsub_<versão>_amd64.deb
# Fedora/RHEL
sudo rpm -i redis-conflated-pubsub-<versão>-1.x86_64.rpm
# Alpine
sudo apk add --allow-untrusted redis-conflated-pubsub_<versão>_x86_64.apk
```

O pacote **não** inicia o serviço por conta própria: ele cria a conta de serviço,
recarrega o systemd e imprime o comando para você habilitar.

```sh
sudo systemctl enable --now redis-conflated-pubsub.service
```

| Caminho | O quê |
| --- | --- |
| `/usr/bin/redis-conflated-pubsub` | o binário |
| `/etc/redis-conflated-pubsub/config.json` | configuração inicial; um upgrade nunca sobrescreve |
| `/etc/redis-conflated-pubsub/redis.env` | segredos das variáveis `*_env`, modo `0600`, lido pelo systemd |
| `/usr/lib/systemd/system/redis-conflated-pubsub.service` | unidade systemd (Debian, RPM, Arch) |
| `/usr/lib/sysusers.d/redis-conflated-pubsub.conf` | conta de serviço dedicada, criada sem precisar de `shadow-utils` |
| `/etc/init.d/redis-conflated-pubsub` | script OpenRC (Alpine, que não tem systemd) |

A unidade roda como o usuário `redis-conflated-pubsub`, com estado em
`/var/lib/redis-conflated-pubsub`, `EnvironmentFile` apontando para o `redis.env`,
`Restart=on-failure` e endurecimento (`NoNewPrivileges`, `PrivateTmp`,
`ProtectSystem=strict`, `ProtectHome`). O log vai para o journal.

## Contêiner

A imagem do relay e o bundle com Redis/Valkey em um contêiner estão em
[Docker e testes](docker.md).
