; german translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.

[CustomMessages]
GroupService=Windows-Dienst
GroupShortcuts=Verknüpfungen
TaskServiceDesc=Den Windows-Dienst installieren und starten (empfohlen)
TaskServiceAutoDesc=Den Dienst beim Start von Windows automatisch starten
TaskDesktopIconDesc=Eine Desktop-Verknüpfung zum Überprüfen der Konfiguration erstellen
PageConfigTitle=Konfiguration
PageConfigSubtitle=Welche Konfiguration soll das Relay verwenden?
PageConfigDesc=Das Relay liest eine einzelne JSON-Datei. Der Standardpfad wird für Sie aus config.example.json mit absoluten Pfaden für Log, Lock und Status erstellt, weil ein Windows-Dienst mit System32 als Arbeitsverzeichnis startet. Die Überprüfung verwendet die Option --check-config des Relays selbst.%n%nVerweisen Sie auf eine vorhandene Datei, um Ihre eigene Konfiguration beizubehalten.
ConfigPathLabel=Konfigurationsdatei
ValidateOk=Die Konfiguration ist gültig.
ValidateFailed=Das Relay hat diese Konfiguration abgelehnt:%n%n%1%n%nTrotzdem installieren? Der Dienst kann nicht gestartet werden, bis die Datei korrigiert wurde.
ValidateMissing=Wählen Sie einen Pfad für die Konfigurationsdatei.
ConfigMissing=Die Konfigurationsdatei existiert nicht:%n%n%1
ConfigCreated=config.json wurde aus config.example.json mit absoluten Pfaden geschrieben.
ConfigKept=Eine vorhandene Konfigurationsdatei wurde unverändert beibehalten.
ServiceInstallFailed=Der Windows-Dienst konnte nicht installiert werden:%n%n%1
ServiceStartFailed=Der Dienst wurde installiert, konnte aber nicht gestartet werden:%n%n%1
ServiceReady=Der Windows-Dienst "{#ServiceDisplayName}" ist installiert und wird ausgeführt.
RunEditConfig=config.json im Editor öffnen
RunDocs=Die Projektdokumentation öffnen (3 Sprachen)
IconValidate=Konfiguration überprüfen
IconConfig=config.json bearbeiten
IconDocs=Projektdokumentation
IconUninstall={#AppName} entfernen
UninstKeepConfig=config.json und die Logdateien behalten?%n%nWählen Sie "Nein", um sie zusammen mit der Anwendung zu löschen.
UninstServiceRemoved=Der Windows-Dienst "{#ServiceDisplayName}" wurde entfernt.
UninstServiceFailed=Der Windows-Dienst konnte nicht entfernt werden:%n%n%1
FinishedLine=Richten Sie Ihre Producer auf das konfigurierte Eingabe-Backend aus und beobachten Sie dann das Log-Verzeichnis, um die ersten Nachrichten zu sehen.
ServiceAlive=Das Relay wurde gestartet und hat sich als laufend gemeldet.
ServiceNotServing=Der Dienst wird ausgeführt, aber das Relay hat sich nicht innerhalb von %1 Sekunden als laufend gemeldet. Prüfen Sie das Log-Verzeichnis, um die Ursache zu ermitteln.

; --- Seiten des Konfigurations-Assistenten ---
PageInputTitle=Eingabe
PageInputDesc=Woher die Nachrichten kommen. Ein Redis-Endpunkt wird als host:port geschrieben; fügen Sie das Schema rediss:// hinzu, um TLS zu aktivieren (rediss://redis.example.net:6380). Das Muster verwendet die Glob-Semantik von Redis, daher stimmt feed:* mit jedem Kanal überein, der mit feed: beginnt.
InEndpointLabel=Redis-Endpunkt (host:port)
InDatabaseLabel=Datenbank
InPasswordEnvLabel=Name der Umgebungsvariable mit dem Passwort (optional)
InPatternLabel=Muster des Abonnements
PageOutputTitle=Ausgabe
PageOutputDesc=Wohin die Nachrichten veröffentlicht werden. Präfix und Suffix werden um den Kanalnamen platziert. Sie hindern das Relay außerdem daran, seine eigene Ausgabe zu lesen: Wenn die Eingabe und die Ausgabe derselbe Redis-Server sind, ist mindestens eines von beiden erforderlich, andernfalls verweigert das Relay den Start mit einem Echo-Schleifen-Fehler.
OutEndpointLabel=Redis-Endpunkt (host:port)
OutDatabaseLabel=Datenbank
OutPasswordEnvLabel=Name der Umgebungsvariable mit dem Passwort (optional)
OutChannelPrefixLabel=Kanalpräfix
OutChannelSuffixLabel=Kanalsuffix
PageTuningTitle=Feinabstimmung
PageTuningDesc=Wie stark das Relay den Feed komprimiert. Bei einem Conflation-Intervall über Null wird innerhalb jedes Fensters nur der letzte Wert pro Kanal veröffentlicht; Null veröffentlicht jede Nachricht sofort. Eine Deduplizierungs-TTL über Null unterdrückt aufeinanderfolgende identische Payloads, bis das Fenster abläuft; Null deaktiviert sie.
TunIntervalLabel=Conflation-Intervall (ms, 0 veröffentlicht direkt)
TunTtlLabel=Deduplizierungs-TTL (ms, 0 deaktiviert)
TunStatusLabel=Status-HTTP-Port (0 deaktiviert den Listener)
TunLogLevelLabel=Log-Level (error, warn, info, debug, trace)
BadNumber=%1 muss eine ganze Zahl größer oder gleich Null sein.
BadLogLevel=Unbekannter Log-Level. Verwenden Sie error, warn, info, debug oder trace.
GenerateFailed=Die Konfiguration konnte nicht erstellt werden:%n%n%1
ConfigGenerated=Die Konfiguration ist gültig und wird in %1 geschrieben.

; --- Modusauswahl und NATS-Seiten ---
PageModeTitle=Was soll der Assistent tun?
PageModeDesc=Wählen Sie, wie das Relay konfiguriert werden soll. "Eine vorhandene Datei verwenden" überspringt alle Fragen und übernimmt eine config.json, die Sie bereits haben; die anderen vier Optionen erstellen eine config.json für die jeweilige Backend-Kombination und stellen nur die Fragen, die diese Kombination benötigt.
ModeExisting=Eine bereits vorhandene config.json verwenden
ModeRedisRedis=Redis-Eingabe zu Redis-Ausgabe konfigurieren
ModeNatsNats=NATS-Eingabe zu NATS-Ausgabe konfigurieren
ModeRedisNats=Redis-Eingabe zu NATS-Ausgabe konfigurieren
ModeNatsRedis=NATS-Eingabe zu Redis-Ausgabe konfigurieren
PageNatsInTitle=NATS-Eingabe
PageNatsInDesc=Seed-Server durch Kommas oder Leerzeichen getrennt. Jede URL benötigt ein Schema: nats:// für TCP, tls:// für TLS, ws:// oder wss:// für WebSocket, und eine Liste darf WebSocket nicht mit den anderen mischen. Das Subject verwendet die Wildcards von NATS: * stimmt mit genau einem Token überein, und > stimmt mit dem Rest überein und muss am Ende stehen.
PageNatsOutTitle=NATS-Ausgabe
PageNatsOutDesc=Dieselben Server- und Authentifizierungsregeln wie für die Eingabe. Präfix und Suffix werden um das Subject platziert, und die endgültig zugeordnete Zeichenfolge muss ein konkretes Subject sein: Wildcards werden abgelehnt.
NatsServersLabel=NATS-Server (durch Kommas getrennt)
NatsAuthLabel=Authentifizierung (none, user_password, token, credentials_file)
NatsUsernameLabel=Benutzername (user_password)
NatsPasswordEnvLabel=Umgebungsvariable für das Passwort (user_password)
NatsTokenEnvLabel=Umgebungsvariable für das Token (token)
NatsCredentialsLabel=Pfad zur .creds-Datei (credentials_file)
NatsSubjectLabel=Subject des Abonnements
BadAuthType=Unbekannter Authentifizierungstyp. Verwenden Sie none, user_password, token oder credentials_file.
SelectorMissing=Geben Sie das Muster oder Subject ein, das dieses Abonnement empfangen soll. Das Relay lehnt einen leeren Selektor ab.
ModeInstallOnly=Nur die ausführbare Datei installieren (ohne Konfiguration)
FinishedNoConfig=Es wurde keine Konfiguration erstellt und der Dienst wurde nicht installiert: Gehen Sie von config.example.json oder config.nats.example.json aus und führen Sie scripts\service.ps1 im Installationsordner aus, sobald Sie bereit sind.
