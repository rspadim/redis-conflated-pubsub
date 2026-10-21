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

; --- 構成ウィザードのページ ---
PageInputTitle=入力
PageInputDesc=メッセージの取得元です。Redis エンドポイントは host:port の形式で記述します。TLS を有効にするには rediss:// スキームを追加します (rediss://redis.example.net:6380)。パターンは Redis の glob セマンティクスに従うため、feed:* は feed: で始まるすべてのチャネルに一致します。
InEndpointLabel=Redis エンドポイント (host:port)
InDatabaseLabel=データベース
InPasswordEnvLabel=パスワードを保持する環境変数の名前 (省略可能)
InPatternLabel=サブスクリプションパターン
PageOutputTitle=出力
PageOutputDesc=メッセージの公開先です。プレフィックスとサフィックスはチャネル名の前後に付加されます。また、relay が自身の出力を読み取らないようにする役割もあります。入力と出力が同じ Redis サーバーの場合、少なくとも一方を指定する必要があります。そうしないと、relay はエコーループエラーで起動を拒否します。
OutEndpointLabel=Redis エンドポイント (host:port)
OutDatabaseLabel=データベース
OutPasswordEnvLabel=パスワードを保持する環境変数の名前 (省略可能)
OutChannelPrefixLabel=チャネルプレフィックス
OutChannelSuffixLabel=チャネルサフィックス
PageTuningTitle=チューニング
PageTuningDesc=relay がフィードをどれだけ圧縮するかの設定です。conflation 間隔が 0 より大きい場合、各ウィンドウ内ではチャネルごとに最新の値だけが公開されます。0 の場合はすべてのメッセージが即座に公開されます。重複排除 TTL が 0 より大きい場合、ウィンドウが期限切れになるまで連続する同一ペイロードが抑止されます。0 にすると無効になります。
TunIntervalLabel=conflation 間隔 (ms、0 で直接公開)
TunTtlLabel=重複排除 TTL (ms、0 で無効)
TunStatusLabel=ステータス HTTP ポート (0 でリスナーを無効化)
TunLogLevelLabel=ログレベル (error、warn、info、debug、trace)
BadNumber=%1 は 0 以上の整数である必要があります。
BadLogLevel=不明なログレベルです。error、warn、info、debug、trace のいずれかを使用してください。
GenerateFailed=構成を生成できませんでした:%n%n%1
ConfigGenerated=構成は有効です。%1 に書き込まれます。

; --- モードメニューと NATS ページ ---
PageModeTitle=ウィザードで何を行いますか？
PageModeDesc=relay の構成方法を選択してください。「既存のファイルを使用する」を選ぶと、すべての質問をスキップして既にお持ちの config.json を使用します。残りの 4 つのオプションは、そのバックエンドの組み合わせに合わせて構成を生成し、その組み合わせに必要な質問だけを行います。
ModeExisting=既存の config.json を使用する
ModeRedisRedis=Redis 入力を Redis 出力に構成する
ModeNatsNats=NATS 入力を NATS 出力に構成する
ModeRedisNats=Redis 入力を NATS 出力に構成する
ModeNatsRedis=NATS 入力を Redis 出力に構成する
PageNatsInTitle=NATS 入力
PageNatsInDesc=シードサーバーはカンマまたはスペースで区切って指定します。各 URL にはスキームが必要です。TCP には nats://、TLS には tls://、WebSocket には ws:// または wss:// を指定します。1 つのリストに WebSocket とそれ以外を混在させることはできません。サブジェクトには NATS のワイルドカードを使用します。* はちょうど 1 つのトークンに一致し、> は残りすべてに一致し、最後に指定する必要があります。
PageNatsOutTitle=NATS 出力
PageNatsOutDesc=サーバーと認証の規則は入力と同じです。プレフィックスとサフィックスはサブジェクトの前後に付加され、最終的にマッピングされた文字列は具体的なサブジェクトである必要があります。ワイルドカードは拒否されます。
NatsServersLabel=NATS サーバー (カンマ区切り)
NatsAuthLabel=認証 (none、user_password、token、credentials_file)
NatsUsernameLabel=ユーザー名 (user_password)
NatsPasswordEnvLabel=パスワードの環境変数 (user_password)
NatsTokenEnvLabel=トークンの環境変数 (token)
NatsCredentialsLabel=.creds ファイルのパス (credentials_file)
NatsSubjectLabel=サブスクリプションのサブジェクト
BadAuthType=不明な認証タイプです。none、user_password、token、credentials_file のいずれかを使用してください。
SelectorMissing=このサブスクリプションが受信するパターンまたはサブジェクトを入力してください。relay は空のセレクターを拒否します。
ModeInstallOnly=実行可能ファイルのみをインストールする (構成なし)
FinishedNoConfig=構成は作成されず、サービスもインストールされませんでした。準備ができたら、config.example.json または config.nats.example.json から始め、インストールディレクトリで scripts\service.ps1 を実行してください。
SecretValueLabel=パスワードの値 (サービス自身の環境に保存されます。変数を自分で設定する場合は空のままにしてください)
SecretsStored=サービスの環境に保存しました: %1
SecretsFailed=サービスの環境を更新できませんでした:%n%n%1
SecretNotSetWarning=%1 に値が入力されなかったため、インストーラーはこの値を保存しません。relay は認証情報をサービス自身の環境から読み取りますが、そこにこの変数はまだ存在しません。この変数を設定するまで、サービスは起動時に終了します。続行しますか？

; --- エンドポイントページの「接続をテスト」ボタン ---
ProbeButton=接続をテスト
ProbeTesting=テスト中...（最大 %1 秒待機）
ProbeOk=接続済み - %1 のサーバーが応答しました。
ProbeUnknownHost=接続できませんでした: ホスト名が不明です。アドレスの入力ミスを確認してください。
ProbeTimeout=接続できませんでした: 時間内に応答がありません。サーバーは起動して到達可能ですか（ファイアウォール、VPN）？
ProbeAuthFailed=サーバーが認証情報を拒否しました。ユーザー名とパスワードの値を確認してください。
ProbeUnexpected=このアドレスから応答はありましたが、想定したサーバーではありません。
ProbeNoWebSocket=このアドレスは、このチェックが対応していない WebSocket を使用しています。リレーの --check-config を使うか、インストール後にテストしてください。
ProbeEmpty=テストする前に入力してください。
ProbeRefused=接続できませんでした: 接続が拒否されました。そのポートでサーバーは起動していますか？
