; Brazilian Portuguese translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.
;
[CustomMessages]
GroupService=Serviço do Windows
GroupShortcuts=Atalhos
TaskServiceDesc=Instalar e iniciar o serviço do Windows (recomendado)
TaskServiceAutoDesc=Iniciar o serviço automaticamente com o Windows
TaskDesktopIconDesc=Criar um atalho na área de trabalho para validar a configuração
PageConfigTitle=Arquivo de configuração
PageConfigSubtitle=Onde o config.json será gravado
PageConfigDesc=Se o arquivo ainda não existir, ele é gerado a partir das suas respostas. Se já existir, ele é mantido exatamente como está — aponte para um arquivo existente para usar a sua própria configuração.

; --- paginas do wizard de configuracao ---
PageInputTitle=Entrada
PageInputDesc=De onde as mensagens vêm. O endpoint do Redis é escrito como host:porta; use o esquema rediss:// para habilitar TLS (rediss://redis.exemplo.net:6380). O padrão usa a semântica glob do Redis, então feed:* casa com todo canal que começa com feed:.
InEndpointLabel=Endpoint do Redis (host:porta)
InDatabaseLabel=Banco
InPasswordEnvLabel=Nome da variável de ambiente com a senha (opcional)
InPatternLabel=Padrão de inscrição
PageOutputTitle=Saída
PageOutputDesc=Onde as mensagens são publicadas. O prefixo e o sufixo são colocados em volta do nome do canal. Eles também impedem que o relay leia a própria saída: quando a entrada e a saída são o mesmo servidor Redis, pelo menos um deles é obrigatório, senão o relay se recusa a iniciar com erro de eco.
OutEndpointLabel=Endpoint do Redis (host:porta)
OutDatabaseLabel=Banco
OutPasswordEnvLabel=Nome da variável de ambiente com a senha (opcional)
OutChannelPrefixLabel=Prefixo do canal
OutChannelSuffixLabel=Sufixo do canal
PageTuningTitle=Ajustes
PageTuningDesc=Quanto o relay comprime o feed. Com intervalo de conflação acima de zero, só o último valor por canal é publicado dentro de cada janela; zero publica cada mensagem imediatamente. Com TTL de deduplicação acima de zero, payloads idênticos consecutivos são suprimidos até a janela expirar; zero desativa.
TunIntervalLabel=Intervalo de conflação (ms, 0 publica direto)
TunTtlLabel=TTL de deduplicação (ms, 0 desativa)
TunStatusLabel=Porta HTTP de status (0 desativa o listener)
TunLogLevelLabel=Nível de log (error, warn, info, debug, trace)
BadNumber=%1 precisa ser um número inteiro de zero ou mais.
BadLogLevel=Nível de log desconhecido. Use error, warn, info, debug ou trace.
GenerateFailed=Não foi possível gerar a configuração:%n%n%1
ConfigGenerated=A configuração é válida e será gravada em %1.

; --- menu de modo e paginas NATS ---
PageModeTitle=O que o wizard deve fazer?
PageModeDesc=Escolha como configurar o relay. "Usar um arquivo existente" pula todas as perguntas e usa um config.json que você já tem; as outras quatro opções geram um para aquela combinação de backends, perguntando só o que aquela combinação precisa.
ModeExisting=Usar um config.json que já existe
ModeRedisRedis=Configurar entrada Redis para saída Redis
ModeNatsNats=Configurar entrada NATS para saída NATS
ModeRedisNats=Configurar entrada Redis para saída NATS
ModeNatsRedis=Configurar entrada NATS para saída Redis
PageNatsInTitle=Entrada NATS
PageNatsInDesc=Servidores de seed separados por vírgula ou espaço. Toda URL precisa de esquema: nats:// para TCP, tls:// para TLS, ws:// ou wss:// para WebSocket, e uma mesma lista não pode misturar WebSocket com os outros. O subject usa os curingas do NATS: * casa exatamente um token, e > casa o resto e precisa ser o último.
PageNatsOutTitle=Saída NATS
PageNatsOutDesc=As mesmas regras de servidores e autenticação da entrada. O prefixo e o sufixo são colocados em volta do subject, e a string final precisa ser um subject concreto: curingas são rejeitados.
NatsServersLabel=Servidores NATS (separados por vírgula)
NatsAuthLabel=Autenticação (none, user_password, token, credentials_file)
NatsUsernameLabel=Usuário (user_password)
NatsPasswordEnvLabel=Variável de ambiente da senha (user_password)
NatsTokenEnvLabel=Variável de ambiente do token (token)
NatsCredentialsLabel=Caminho do arquivo .creds (credentials_file)
NatsSubjectLabel=Subject da inscrição
BadAuthType=Tipo de autenticação desconhecido. Use none, user_password, token ou credentials_file.
SelectorMissing=Informe o padrão ou o subject desta inscrição. O relay rejeita um selector vazio.
SecretValueLabel=Valor da senha (gravado no ambiente do próprio serviço; deixe vazio para definir a variável você mesmo)
SecretsStored=Gravado no ambiente do serviço: %1
SecretsFailed=Não foi possível atualizar o ambiente do serviço:%n%n%1
SecretNotSetWarning=Nenhum valor foi informado para %1, então o instalador não vai gravá-la. O relay lê as credenciais do ambiente do próprio serviço, onde essa variável ainda não existe: o serviço vai sair na inicialização até você defini-la. Continuar?
ModeInstallOnly=Instalar somente o executável (sem configurar)
FinishedNoConfig=Nenhuma configuração foi criada e o serviço não foi instalado: parta de config.example.json ou config.nats.example.json e rode scripts\service.ps1 na pasta de instalação quando estiver pronto.
ConfigPathLabel=Arquivo de configuração
ValidateOk=A configuração é válida.
ValidateFailed=O relay rejeitou esta configuração:%n%n%1%n%nInstalar mesmo assim? O serviço não vai iniciar até o arquivo ser corrigido.
ValidateMissing=Escolha o caminho de um arquivo de configuração.
ConfigMissing=O arquivo de configuração não existe:%n%n%1
ConfigCreated=config.json foi gravado a partir de config.example.json com caminhos absolutos.
ConfigKept=O arquivo de configuração existente foi mantido.
ServiceInstallFailed=O serviço do Windows não pôde ser instalado:%n%n%1
ServiceStartFailed=O serviço foi instalado mas não pôde ser iniciado:%n%n%1
ServiceReady=O serviço do Windows "{#ServiceDisplayName}" está instalado e em execução.
RunEditConfig=Abrir config.json no Bloco de Notas
RunDocs=Abrir a documentação do projeto (3 idiomas)
IconValidate=Validar a configuração
IconConfig=Editar config.json
IconDocs=Documentação do projeto
IconUninstall=Desinstalar o {#AppName}
UninstKeepConfig=Manter config.json e os arquivos de log?%n%nEscolha Não para apagá-los junto com a aplicação.
UninstServiceRemoved=O serviço do Windows "{#ServiceDisplayName}" foi removido.
UninstServiceFailed=O serviço do Windows não pôde ser removido:%n%n%1
FinishedLine=Aponte seus produtores para o backend de entrada configurado e acompanhe o diretório de logs para ver as primeiras mensagens.
ServiceAlive=O relay iniciou e se reportou como em execução.
ServiceNotServing=O serviço está em execução, mas o relay não se reportou como em execução em %1 segundos. Veja o diretório de logs para saber o motivo.
