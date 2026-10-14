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
