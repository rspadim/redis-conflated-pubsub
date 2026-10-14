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
