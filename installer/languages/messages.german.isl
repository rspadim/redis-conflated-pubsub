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
