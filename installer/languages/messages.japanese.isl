; japanese translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.

[CustomMessages]
GroupService=Windows サービス
GroupShortcuts=ショートカット
TaskServiceDesc=Windows サービスをインストールして開始する (推奨)
TaskServiceAutoDesc=Windows の起動時にサービスを自動的に開始する
TaskDesktopIconDesc=構成を検証するためのデスクトップショートカットを作成する
PageConfigTitle=構成
PageConfigSubtitle=relay が使用する構成を指定してください。
PageConfigDesc=relay は単一の JSON ファイルを読み取ります。既定のパスは config.example.json から、log、lock、status の絶対パスを指定して作成されます。Windows サービスは作業ディレクトリが System32 に設定された状態で起動するためです。検証には relay 自身の --check-config を使用します。%n%n既存のファイルを指定すると、独自の構成をそのまま維持できます。
ConfigPathLabel=構成ファイル
ValidateOk=構成は有効です。
ValidateFailed=relay はこの構成を拒否しました:%n%n%1%n%nこのままインストールしますか？ファイルが修正されるまで、サービスは起動に失敗します。
ValidateMissing=構成ファイルのパスを選択してください。
ConfigMissing=構成ファイルが存在しません:%n%n%1
ConfigCreated=config.json を config.example.json から絶対パス付きで作成しました。
ConfigKept=既存の構成ファイルは変更されずにそのまま維持されました。
ServiceInstallFailed=Windows サービスをインストールできませんでした:%n%n%1
ServiceStartFailed=サービスはインストールされましたが、開始できませんでした:%n%n%1
ServiceReady=Windows サービス "{#ServiceDisplayName}" はインストールされ、実行中です。
RunEditConfig=config.json をメモ帳で開く
RunDocs=プロジェクトのドキュメントを開く (3 言語)
IconValidate=構成を検証する
IconConfig=config.json を編集する
IconDocs=プロジェクトのドキュメント
IconUninstall={#AppName} をアンインストールする
UninstKeepConfig=config.json とログファイルを維持しますか？%n%nアプリケーションと一緒に削除するには「いいえ」を選択してください。
UninstServiceRemoved=Windows サービス "{#ServiceDisplayName}" を削除しました。
UninstServiceFailed=Windows サービスを削除できませんでした:%n%n%1
FinishedLine=プロデューサーを構成済みの入力バックエンドに接続し、ログディレクトリで最初のメッセージを確認してください。
ServiceAlive=relay が起動し、実行中であることを報告しました。
ServiceNotServing=サービスは実行中ですが、relay は %1 秒以内に実行中であることを報告しませんでした。原因を確認するにはログディレクトリを参照してください。
