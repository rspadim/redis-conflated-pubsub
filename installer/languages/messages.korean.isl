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

; --- 구성 마법사의 새 페이지 메시지 ---
PageInputTitle=입력
PageInputDesc=메시지가 들어오는 위치입니다. Redis 엔드포인트는 host:port 형식으로 작성하며, TLS를 사용하려면 rediss:// 스킴을 추가하세요 (rediss://redis.example.net:6380). 패턴은 Redis glob 규칙을 따르므로 feed:*는 feed:로 시작하는 모든 채널과 일치합니다.
InEndpointLabel=Redis 엔드포인트 (host:port)
InDatabaseLabel=데이터베이스
InPasswordEnvLabel=암호를 담은 환경 변수 이름 (선택 사항)
InPatternLabel=구독 패턴
PageOutputTitle=출력
PageOutputDesc=메시지가 게시되는 위치입니다. 접두사와 접미사는 채널 이름 앞뒤에 배치됩니다. 또한 relay가 자체 출력을 읽지 못하도록 막아 줍니다. 입력과 출력이 같은 Redis 서버일 때는 둘 중 하나 이상이 필요하며, 그렇지 않으면 relay가 에코 루프 오류와 함께 시작을 거부합니다.
OutEndpointLabel=Redis 엔드포인트 (host:port)
OutDatabaseLabel=데이터베이스
OutPasswordEnvLabel=암호를 담은 환경 변수 이름 (선택 사항)
OutChannelPrefixLabel=채널 접두사
OutChannelSuffixLabel=채널 접미사
PageTuningTitle=세부 조정
PageTuningDesc=relay가 피드를 얼마나 압축할지 정합니다. 병합(conflation) 간격이 0보다 크면 각 구간에서 채널별 최신 값만 게시하고, 0이면 모든 메시지를 즉시 게시합니다. 중복 제거 TTL이 0보다 크면 구간이 만료될 때까지 연속으로 동일한 페이로드를 억제하고, 0이면 사용하지 않습니다.
TunIntervalLabel=병합 간격 (ms, 0이면 즉시 게시)
TunTtlLabel=중복 제거 TTL (ms, 0이면 비활성화)
TunStatusLabel=상태 HTTP 포트 (0이면 리스너 비활성화)
TunLogLevelLabel=로그 수준 (error, warn, info, debug, trace)
BadNumber=%1은(는) 0 이상의 정수여야 합니다.
BadLogLevel=알 수 없는 로그 수준입니다. error, warn, info, debug 또는 trace를 사용하세요.
GenerateFailed=구성을 생성할 수 없습니다:%n%n%1
ConfigGenerated=구성이 유효하며 %1에 기록됩니다.
PageModeTitle=마법사가 무엇을 해야 합니까?
PageModeDesc=relay를 구성할 방식을 선택하세요. "기존 파일 사용"은 모든 질문을 건너뛰고 이미 가지고 있는 config.json을 사용합니다. 나머지 네 가지 옵션은 해당 백엔드 조합에 맞게 구성을 생성하며, 그 조합에 필요한 질문만 합니다.
ModeExisting=이미 존재하는 config.json 사용
ModeRedisRedis=Redis 입력을 Redis 출력으로 구성
ModeNatsNats=NATS 입력을 NATS 출력으로 구성
ModeRedisNats=Redis 입력을 NATS 출력으로 구성
ModeNatsRedis=NATS 입력을 Redis 출력으로 구성
PageNatsInTitle=NATS 입력
PageNatsInDesc=쉼표 또는 공백으로 구분된 시드 서버입니다. 모든 URL에는 스킴이 필요합니다. TCP에는 nats://, TLS에는 tls://, WebSocket에는 ws:// 또는 wss://를 사용하며, 하나의 목록에 WebSocket과 다른 스킴을 함께 사용할 수 없습니다. subject는 NATS 와일드카드를 사용합니다. *는 토큰 하나와 정확히 일치하고, >는 나머지와 일치하며 마지막에 와야 합니다.
PageNatsOutTitle=NATS 출력
PageNatsOutDesc=서버와 인증 규칙은 입력과 동일합니다. 접두사와 접미사는 subject 앞뒤에 배치되며, 최종 매핑된 문자열은 구체적인 subject여야 합니다. 와일드카드는 거부됩니다.
NatsServersLabel=NATS 서버 (쉼표로 구분)
NatsAuthLabel=인증 (none, user_password, token, credentials_file)
NatsUsernameLabel=사용자 이름 (user_password)
NatsPasswordEnvLabel=암호 환경 변수 (user_password)
NatsTokenEnvLabel=토큰 환경 변수 (token)
NatsCredentialsLabel=자격 증명 파일 경로 (credentials_file)
NatsSubjectLabel=구독 subject
BadAuthType=알 수 없는 인증 유형입니다. none, user_password, token 또는 credentials_file 중 하나를 사용하세요.
SelectorMissing=이 구독이 받을 패턴 또는 subject를 입력하세요. relay는 빈 선택자를 거부합니다.
ModeInstallOnly=실행 파일만 설치 (구성 없음)
FinishedNoConfig=구성이 만들어지지 않았고 서비스도 설치되지 않았습니다. 준비가 되면 config.example.json 또는 config.nats.example.json에서 시작하고 설치 디렉터리에서 scripts\service.ps1을 실행하세요.

; --- 자격 증명을 서비스 환경에 저장 ---
SecretValueLabel=암호 값 (서비스 자체의 환경에 저장됩니다; 변수를 직접 설정하려면 비워 두세요)
SecretsStored=서비스 환경에 저장됨: %1
SecretsFailed=서비스 환경을 업데이트할 수 없습니다:%n%n%1
SecretNotSetWarning=%1에 값을 입력하지 않았으므로 설치 프로그램이 값을 저장하지 않습니다. relay는 자격 증명을 서비스 자체의 환경에서 읽지만, 그 환경에는 해당 변수가 아직 존재하지 않습니다. 변수를 설정할 때까지 서비스는 시작할 때 종료됩니다. 계속하시겠습니까?
