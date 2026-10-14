; chinesesimplified translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.

[CustomMessages]
GroupService=Windows 服务
GroupShortcuts=快捷方式
TaskServiceDesc=安装并启动 Windows 服务（推荐）
TaskServiceAutoDesc=在 Windows 启动时自动启动服务
TaskDesktopIconDesc=创建用于验证配置的桌面快捷方式
PageConfigTitle=配置
PageConfigSubtitle=relay 应使用哪个配置？
PageConfigDesc=relay 读取单个 JSON 文件。默认路径将根据 config.example.json 为您生成，包含 log、lock 和 status 的绝对路径，因为 Windows 服务启动时其工作目录为 System32。验证使用 relay 自带的 --check-config。%n%n指向现有文件即可保留您自己的配置。
ConfigPathLabel=配置文件
ValidateOk=配置有效。
ValidateFailed=relay 拒绝了此配置：%n%n%1%n%n仍要安装吗？在修复该文件之前，服务将无法启动。
ValidateMissing=请选择配置文件的路径。
ConfigMissing=配置文件不存在：%n%n%1
ConfigCreated=已根据 config.example.json 生成 config.json，并使用绝对路径。
ConfigKept=保留了现有的配置文件，未做任何修改。
ServiceInstallFailed=无法安装 Windows 服务：%n%n%1
ServiceStartFailed=服务已安装，但无法启动：%n%n%1
ServiceReady=Windows 服务“{#ServiceDisplayName}”已安装并正在运行。
RunEditConfig=用记事本打开 config.json
RunDocs=打开项目文档（3 种语言）
IconValidate=验证配置
IconConfig=编辑 config.json
IconDocs=项目文档
IconUninstall=卸载 {#AppName}
UninstKeepConfig=是否保留 config.json 和日志文件？%n%n选择“否”将随应用程序一起删除它们。
UninstServiceRemoved=Windows 服务“{#ServiceDisplayName}”已移除。
UninstServiceFailed=无法移除 Windows 服务：%n%n%1
FinishedLine=请将您的生产者指向已配置的输入后端，然后观察日志目录以查看首批消息。
ServiceAlive=relay 已启动并报告自身正在运行。
ServiceNotServing=服务正在运行，但 relay 未在 %1 秒内报告自身正在运行。请检查日志目录以了解原因。

; --- configuration wizard pages ---
PageInputTitle=输入
PageInputDesc=消息从哪里来。Redis 端点写成 host:port 的形式；添加 rediss:// 方案即可启用 TLS（rediss://redis.example.net:6380）。匹配模式使用 Redis 的 glob 语义，因此 feed:* 匹配所有以 feed: 开头的频道。
InEndpointLabel=Redis 端点（host:port）
InDatabaseLabel=数据库
InPasswordEnvLabel=存放密码的环境变量名称（可选）
InPatternLabel=订阅匹配模式
PageOutputTitle=输出
PageOutputDesc=消息发布到哪里。前缀和后缀会被放在频道名称的两侧。它们还能防止 relay 读取自己的输出：当输入和输出是同一个 Redis 服务器时，必须至少设置其中之一，否则 relay 会以 echo-loop 错误拒绝启动。
OutEndpointLabel=Redis 端点（host:port）
OutDatabaseLabel=数据库
OutPasswordEnvLabel=存放密码的环境变量名称（可选）
OutChannelPrefixLabel=频道前缀
OutChannelSuffixLabel=频道后缀
PageTuningTitle=调优
PageTuningDesc=relay 对消息流的合并程度。合并间隔大于零时，每个窗口内每个频道只发布最新的值；为零则立即发布每条消息。去重 TTL 大于零时，会抑制连续的相同负载，直到窗口到期；为零则禁用。
TunIntervalLabel=合并间隔（毫秒，0 表示直接发布）
TunTtlLabel=去重 TTL（毫秒，0 表示禁用）
TunStatusLabel=状态 HTTP 端口（0 表示禁用监听器）
TunLogLevelLabel=日志级别（error、warn、info、debug、trace）
BadNumber=%1 必须是大于或等于零的整数。
BadLogLevel=未知的日志级别。请使用 error、warn、info、debug 或 trace。
GenerateFailed=无法生成配置：%n%n%1
ConfigGenerated=配置有效，将写入 %1。

; --- mode menu and NATS pages ---
PageModeTitle=向导应执行什么操作？
PageModeDesc=选择如何配置 relay。“使用现有文件”会跳过所有问题，直接采用您已有的 config.json；其余四个选项会为该后端组合生成配置，只询问该组合需要的问题。
ModeExisting=使用已存在的 config.json
ModeRedisRedis=配置 Redis 输入到 Redis 输出
ModeNatsNats=配置 NATS 输入到 NATS 输出
ModeRedisNats=配置 Redis 输入到 NATS 输出
ModeNatsRedis=配置 NATS 输入到 Redis 输出
PageNatsInTitle=NATS 输入
PageNatsInDesc=以逗号或空格分隔的种子服务器。每个 URL 都必须带方案：nats:// 用于 TCP，tls:// 用于 TLS，ws:// 或 wss:// 用于 WebSocket；同一个列表不能将 WebSocket 与其他方案混用。主题使用 NATS 通配符：* 恰好匹配一个 token，> 匹配其余部分并且必须放在最后。
PageNatsOutTitle=NATS 输出
PageNatsOutDesc=服务器和认证规则与输入相同。前缀和后缀会被放在主题的两侧，最终映射出的字符串必须是具体的主题：不接受通配符。
NatsServersLabel=NATS 服务器（以逗号分隔）
NatsAuthLabel=认证方式（none、user_password、token、credentials_file）
NatsUsernameLabel=用户名（user_password）
NatsPasswordEnvLabel=密码环境变量（user_password）
NatsTokenEnvLabel=存放 token 的环境变量（token）
NatsCredentialsLabel=凭据文件路径（credentials_file）
NatsSubjectLabel=订阅主题
BadAuthType=未知的认证方式。请使用 none、user_password、token 或 credentials_file。
SelectorMissing=请输入此订阅应接收的匹配模式或主题。relay 拒绝空的选择器。
ModeInstallOnly=仅安装可执行文件（不创建配置）
FinishedNoConfig=未创建任何配置，服务也未安装：准备就绪后，请从 config.example.json 或 config.nats.example.json 开始，并在安装目录中运行 scripts\service.ps1。
