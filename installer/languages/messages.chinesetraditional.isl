; chinesetraditional translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.

[CustomMessages]
GroupService=Windows 服務
GroupShortcuts=捷徑
TaskServiceDesc=安裝並啟動 Windows 服務 (建議)
TaskServiceAutoDesc=在 Windows 開機時自動啟動服務
TaskDesktopIconDesc=建立桌面捷徑以驗證設定
PageConfigTitle=設定
PageConfigSubtitle=relay 應使用哪個設定？
PageConfigDesc=relay 會讀取單一 JSON 檔案。預設路徑會從 config.example.json 為您建立，並包含絕對的 log、lock 與 status 路徑，因為 Windows 服務啟動時，其工作目錄會設為 System32。驗證會使用 relay 本身的 --check-config。%n%n若要保留您自己的設定，請將其指向現有的檔案。
ConfigPathLabel=設定檔
ValidateOk=設定有效。
ValidateFailed=relay 已拒絕此設定:%n%n%1%n%n仍要安裝嗎？在修正該檔案之前，服務將無法啟動。
ValidateMissing=請選擇設定檔路徑。
ConfigMissing=設定檔不存在:%n%n%1
ConfigCreated=已從 config.example.json 寫入 config.json，並使用絕對路徑。
ConfigKept=已保留現有的設定檔，未做任何變更。
ServiceInstallFailed=無法安裝 Windows 服務:%n%n%1
ServiceStartFailed=服務已安裝，但無法啟動:%n%n%1
ServiceReady=Windows 服務 "{#ServiceDisplayName}" 已安裝並正在執行。
RunEditConfig=在 Notepad 中開啟 config.json
RunDocs=開啟專案文件 (3 種語言)
IconValidate=驗證設定
IconConfig=編輯 config.json
IconDocs=專案文件
IconUninstall=解除安裝 {#AppName}
UninstKeepConfig=要保留 config.json 與記錄檔嗎？%n%n選擇 「否」 會將它們與應用程式一併刪除。
UninstServiceRemoved=Windows 服務 "{#ServiceDisplayName}" 已移除。
UninstServiceFailed=無法移除 Windows 服務:%n%n%1
FinishedLine=將您的生產者指向已設定的輸入後端，然後觀察 logs 目錄以查看最初的訊息。
ServiceAlive=relay 已啟動並回報為執行中。
ServiceNotServing=服務正在執行，但 relay 未在 %1 秒內回報為執行中。請檢查 logs 目錄以了解原因。

; --- configuration wizard pages ---
PageInputTitle=輸入
PageInputDesc=訊息的來源。Redis 端點會寫成主機:連接埠；在開頭加上 rediss:// 即可啟用 TLS (rediss://redis.example.net:6380)。模式採用 Redis glob 語意，因此 feed:* 會比對所有以 feed: 開頭的頻道。
InEndpointLabel=Redis 端點 (主機:連接埠)
InDatabaseLabel=資料庫
InPasswordEnvLabel=存放密碼的環境變數名稱 (選填)
InPatternLabel=訂閱模式
PageOutputTitle=輸出
PageOutputDesc=訊息的發佈位置。前綴與後綴會置於頻道名稱前後。它們也能避免 relay 讀取自己的輸出: 當輸入與輸出是同一部 Redis 伺服器時，至少必須設定其中一個，否則 relay 會因回音迴圈錯誤而拒絕啟動。
OutEndpointLabel=Redis 端點 (主機:連接埠)
OutDatabaseLabel=資料庫
OutPasswordEnvLabel=存放密碼的環境變數名稱 (選填)
OutChannelPrefixLabel=頻道前綴
OutChannelSuffixLabel=頻道後綴
PageTuningTitle=調校
PageTuningDesc=relay 對訊息流的壓縮程度。當彙整間隔大於零時，每個時間範圍內只會發佈各頻道的最新值；零則會立即發佈每一則訊息。當去除重複 TTL 大於零時，會抑制連續相同的承載資料，直到時間範圍到期；零則會停用此功能。
TunIntervalLabel=彙整間隔 (ms，0 表示直接發佈)
TunTtlLabel=去除重複 TTL (ms，0 停用)
TunStatusLabel=狀態 HTTP 連接埠 (0 停用接聽程式)
TunLogLevelLabel=記錄層級 (error、warn、info、debug、trace)
BadNumber=%1 必須是零或以上的整數。
BadLogLevel=未知的記錄層級。請使用 error、warn、info、debug 或 trace。
GenerateFailed=無法產生設定:%n%n%1
ConfigGenerated=設定有效，並將寫入 %1。

; --- mode menu and NATS pages ---
PageModeTitle=精靈要做什麼？
PageModeDesc=選擇要如何設定 relay。「使用現有的檔案」 會略過所有問題，並沿用您現有的 config.json；其他四個選項會為該後端組合產生一份設定，只詢問該組合所需的問題。
ModeExisting=使用現有的 config.json
ModeRedisRedis=設定 Redis 輸入到 Redis 輸出
ModeNatsNats=設定 NATS 輸入到 NATS 輸出
ModeRedisNats=設定 Redis 輸入到 NATS 輸出
ModeNatsRedis=設定 NATS 輸入到 Redis 輸出
PageNatsInTitle=NATS 輸入
PageNatsInDesc=種子伺服器以逗號或空格分隔。每個 URL 都必須有開頭的通訊協定: nats:// 用於 TCP、tls:// 用於 TLS、ws:// 或 wss:// 用於 WebSocket，且同一份清單不能將 WebSocket 與其他通訊協定混用。NATS subject 使用萬用字元: * 僅比對一個權杖，> 則比對其餘部分且必須放在最後。
PageNatsOutTitle=NATS 輸出
PageNatsOutDesc=伺服器與驗證規則和輸入相同。前綴與後綴會置於 subject 前後，且最終對應的字串必須是具體的 subject: 不接受萬用字元。
NatsServersLabel=NATS 伺服器 (以逗號分隔)
NatsAuthLabel=驗證 (none、user_password、token、credentials_file)
NatsUsernameLabel=使用者名稱 (user_password)
NatsPasswordEnvLabel=密碼環境變數 (user_password)
NatsTokenEnvLabel=權杖環境變數 (token)
NatsCredentialsLabel=認證檔案路徑 (credentials_file)
NatsSubjectLabel=訂閱 subject
BadAuthType=未知的驗證類型。請使用 none、user_password、token 或 credentials_file。
SelectorMissing=請輸入此訂閱應接收的模式或 subject。relay 會拒絕空白的 selector。
ModeInstallOnly=僅安裝執行檔 (不進行設定)
FinishedNoConfig=未建立任何設定，且未安裝服務: 請從 config.example.json 或 config.nats.example.json 開始，並在您準備好時，從安裝目錄執行 scripts\service.ps1。

; --- secrets stored in the service's own environment ---
SecretValueLabel=密碼值 (存放於服務本身的環境中；保留空白以自行設定此變數)
SecretsStored=已存放於服務環境: %1
SecretsFailed=無法更新服務環境:%n%n%1
SecretNotSetWarning=未輸入 %1 的值，因此安裝程式不會存放它。relay 會從服務本身的環境讀取其認證，而該變數在該環境中尚不存在: 在您設定該變數之前，服務會在啟動時結束。要繼續嗎？

; --- 端點頁面上的「測試連線」按鈕 ---
ProbeButton=測試連線
ProbeTesting=正在測試...（最多等待 %1 秒）
ProbeOk=已連線 - %1 的伺服器已回應。
ProbeUnknownHost=無法連線：主機名稱未知。請檢查位址是否拼寫錯誤。
ProbeTimeout=無法連線：未在預期時間內回應。伺服器是否在執行且可存取（防火牆、VPN）？
ProbeAuthFailed=伺服器拒絕了認證資訊。請檢查使用者名稱與密碼值。
ProbeUnexpected=該位址有回應，但不是預期的伺服器。
ProbeNoWebSocket=該位址使用 WebSocket，本檢查不支援。請使用中繼的 --check-config，或安裝後再測試。
ProbeEmpty=請先輸入端點再測試。
ProbeRefused=無法連線：連線被拒絕。伺服器是否在該連接埠上執行？
