; korean translation for the installer wizard.
; Keys not listed here fall back to messages.english.isl.

[CustomMessages]
GroupService=Windows 서비스
GroupShortcuts=바로 가기
TaskServiceDesc=Windows 서비스 설치 및 시작 (권장)
TaskServiceAutoDesc=Windows 부팅 시 서비스 자동 시작
TaskDesktopIconDesc=구성을 검증하기 위한 바탕 화면 바로 가기 만들기
PageConfigTitle=구성
PageConfigSubtitle=relay가 어떤 구성을 사용해야 합니까?
PageConfigDesc=relay는 단일 JSON 파일을 읽습니다. Windows 서비스는 작업 디렉터리가 System32로 설정된 상태로 시작하므로, 기본 경로는 log, lock 및 status의 절대 경로를 사용하여 config.example.json에서 자동으로 만들어집니다. 검증에는 relay 자체의 --check-config를 사용합니다.%n%n기존 파일을 지정하면 자신의 구성을 그대로 유지할 수 있습니다.
ConfigPathLabel=구성 파일
ValidateOk=구성이 유효합니다.
ValidateFailed=relay가 이 구성을 거부했습니다:%n%n%1%n%n그래도 설치하시겠습니까? 파일을 수정할 때까지 서비스가 시작되지 않습니다.
ValidateMissing=구성 파일 경로를 선택하세요.
ConfigMissing=구성 파일이 존재하지 않습니다:%n%n%1
ConfigCreated=config.json이 절대 경로를 사용하여 config.example.json에서 작성되었습니다.
ConfigKept=기존 구성 파일이 변경 없이 유지되었습니다.
ServiceInstallFailed=Windows 서비스를 설치할 수 없습니다:%n%n%1
ServiceStartFailed=서비스가 설치되었지만 시작할 수 없습니다:%n%n%1
ServiceReady=Windows 서비스 "{#ServiceDisplayName}"이(가) 설치되어 실행 중입니다.
RunEditConfig=메모장으로 config.json 열기
RunDocs=프로젝트 문서 열기 (3개 언어)
IconValidate=구성 검증
IconConfig=config.json 편집
IconDocs=프로젝트 문서
IconUninstall={#AppName} 제거
UninstKeepConfig=config.json과 로그 파일을 유지하시겠습니까?%n%n응용 프로그램과 함께 삭제하려면 아니요를 선택하세요.
UninstServiceRemoved=Windows 서비스 "{#ServiceDisplayName}"이(가) 제거되었습니다.
UninstServiceFailed=Windows 서비스를 제거할 수 없습니다:%n%n%1
FinishedLine=프로듀서가 구성된 입력 백엔드를 가리키도록 설정한 다음 로그 디렉터리에서 첫 메시지를 확인하세요.
ServiceAlive=relay가 시작되어 실행 중임을 보고했습니다.
ServiceNotServing=서비스는 실행 중이지만 relay가 %1초 이내에 실행 중임을 보고하지 않았습니다. 로그 디렉터리에서 원인을 확인하세요.
