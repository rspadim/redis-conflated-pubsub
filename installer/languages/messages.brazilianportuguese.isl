; Brazilian Portuguese translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.
;
[CustomMessages]
GroupService=Serviço do Windows
GroupShortcuts=Atalhos
TaskServiceDesc=Instalar e iniciar o serviço do Windows (recomendado)
TaskServiceAutoDesc=Iniciar o serviço automaticamente com o Windows
TaskDesktopIconDesc=Criar um atalho na área de trabalho para validar a configuração
PageConfigTitle=Configuração
PageConfigSubtitle=Qual configuração o relay deve usar?
PageConfigDesc=O relay lê um único arquivo JSON. O caminho padrão é criado a partir de config.example.json com caminhos absolutos de log, lock e status, porque um serviço do Windows inicia com o diretório de trabalho em System32. A validação usa o --check-config do próprio relay.%n%nAponte para um arquivo existente para manter a sua configuração.
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
