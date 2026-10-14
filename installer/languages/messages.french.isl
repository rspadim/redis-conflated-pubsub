; french translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.

[CustomMessages]
GroupService=Service Windows
GroupShortcuts=Raccourcis
TaskServiceDesc=Installer et démarrer le service Windows (recommandé)
TaskServiceAutoDesc=Démarrer le service automatiquement au démarrage de Windows
TaskDesktopIconDesc=Créer un raccourci sur le Bureau pour valider la configuration
PageConfigTitle=Configuration
PageConfigSubtitle=Quelle configuration le relay doit-il utiliser ?
PageConfigDesc=Le relay lit un seul fichier JSON. Le chemin par défaut est créé pour vous à partir de config.example.json avec des chemins absolus de log, lock et status, car un service Windows démarre avec son répertoire de travail défini sur System32. La validation utilise le --check-config du relay lui-même.%n%nIndiquez un fichier existant pour conserver votre propre configuration.
ConfigPathLabel=Fichier de configuration
ValidateOk=La configuration est valide.
ValidateFailed=Le relay a rejeté cette configuration :%n%n%1%n%nInstaller quand même ? Le service ne pourra pas démarrer tant que le fichier n'aura pas été corrigé.
ValidateMissing=Choisissez le chemin d'un fichier de configuration.
ConfigMissing=Le fichier de configuration n'existe pas :%n%n%1
ConfigCreated=config.json a été écrit à partir de config.example.json avec des chemins absolus.
ConfigKept=Le fichier de configuration existant a été conservé tel quel.
ServiceInstallFailed=Le service Windows n'a pas pu être installé :%n%n%1
ServiceStartFailed=Le service a été installé, mais n'a pas pu être démarré :%n%n%1
ServiceReady=Le service Windows « {#ServiceDisplayName} » est installé et en cours d'exécution.
RunEditConfig=Ouvrir config.json dans le Bloc-notes
RunDocs=Ouvrir la documentation du projet (3 langues)
IconValidate=Valider la configuration
IconConfig=Modifier config.json
IconDocs=Documentation du projet
IconUninstall=Désinstaller {#AppName}
UninstKeepConfig=Conserver config.json et les fichiers de log ?%n%nChoisissez Non pour les supprimer avec l'application.
UninstServiceRemoved=Le service Windows « {#ServiceDisplayName} » a été supprimé.
UninstServiceFailed=Le service Windows n'a pas pu être supprimé :%n%n%1
FinishedLine=Dirigez vos producteurs vers le backend d'entrée configuré, puis surveillez le répertoire de logs pour voir les premiers messages.
ServiceAlive=Le relay a démarré et a signalé qu'il était en cours d'exécution.
ServiceNotServing=Le service est en cours d'exécution, mais le relay n'a pas signalé qu'il était en cours d'exécution dans les %1 secondes. Consultez le répertoire de logs pour en connaître la raison.
