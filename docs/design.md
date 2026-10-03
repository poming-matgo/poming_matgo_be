# 실행 구조와 설계 근거

[README로 돌아가기](../README.md)

기준은 단일 인스턴스의 `in-memory` 구현입니다. Redis는 같은 저장소 인터페이스를 구현하지만 실행 수명과 취소 보장이 동일하다고 가정하지 않습니다.

## 상태 변경과 안내의 경계

사용자 게임 액션은 `GameWebSocketHandler → WsGameHandler → TurnFlowService → GamePlayService(@GameLock)`로 진행합니다. 자동플레이도 `TurnFlowService`를 사용합니다. 연결별 수신은 `concatMap`으로 처리하고, 서로 다른 연결·자동플레이·HTTP 요청의 경합은 방 단위로 제어합니다.

| 단계 | 책임과 근거 |
| --- | --- |
| 중복 제어 | `InFlightManager`가 NORMAL/AUTOPLAY 키를 분리하고 TTL·소유 토큰으로 관리. 정상 요청은 자동플레이 키에 차단되지 않고, 자동플레이는 정상 요청에 양보 |
| 상호 배제 | `@GameLock`이 in-memory executor·실행 게이트로 위임. 같은 방의 Join·Leave·Ready·선 선택·게임 액션에 공통 적용. 대기 없이 `TRY_AGAIN`, HTTP Join/Leave는 409 |
| 검증·수락 | 락 안에서 최신 phase·플레이어·입력을 검증한 뒤 첫 변경 전에 수락. 수락 전 취소는 중단하고, 수락 후에는 executor가 호출자 취소와 필수 실행을 분리 |
| 필수 완료 | 상태 저장 → 다음 타이머 또는 END 재시작 → 수신자·안내 순서 예약을 락 안에서 완료. STOP도 END를 저장해 낡은 GO 요청이 검증을 통과하지 않도록 함 |
| 송신 | 락을 해제한 뒤 앞선 안내 묶음과 필요한 스냅샷 송신을 기다림. `ActionNotificationScope`가 캡처·대기·송신·예약 회수를 공유 |
| 오류·정리 | 수락 후 오류/실행 timeout은 방 차단·정리로 연결. 전체 정리는 기존 실행을 기다리고 중복 요청을 병합. 안내 실패를 기록하되 필수 데이터·세션 정리는 계속 수행 |

이전의 `RoomLockManager`와 in-memory 선 선택 트리거 맵은 제거했습니다. 준비 시작은 공통 게임 락·최신 phase 검증·락 해제 전 상태 전이로 한 번만 실행합니다. Redis의 `tryClaimLeaderSelectionTrigger` 원자적 선점은 유지하며, in-memory 구현은 상태를 변경하지 않고 true를 반환합니다.

수락한 in-memory 실행은 30초로 제한합니다. 자동플레이의 송신 포함 구독도 별도의 30초 제한을 가지지만, 그 취소가 이미 수락한 필수 처리를 취소한다는 뜻은 아닙니다. 전체 정리는 기존 실행의 남은 기한과 종료 안내 5초 제한을 사용합니다. 동기 메모리 정리에 별도 전체 timeout을 두지 않으며 실제 완료 시간의 상한을 보장하지 않습니다. 정상·오류·취소 시 자원 해제는 `Mono.usingWhen`과 각 자원의 소유권으로 관리합니다.

근거: [TurnFlowService](../game-service/src/main/java/com/pomingmatgo/gameservice/application/game/TurnFlowService.java) · [GamePlayService](../game-service/src/main/java/com/pomingmatgo/gameservice/application/game/GamePlayService.java) · [InMemoryGameActionExecutor](../game-service/src/main/java/com/pomingmatgo/gameservice/application/game/InMemoryGameActionExecutor.java) · [InMemoryRoomExecutionGate](../game-service/src/main/java/com/pomingmatgo/gameservice/infrastructure/lock/InMemoryRoomExecutionGate.java)

## 자동플레이와 방 수명

행동 제한 10초에 서버 유예 2초를 더합니다. `System.nanoTime()`으로 기한을 계산하며 카드 제출·바닥 선택·고/스톱 대기를 모두 처리합니다. 바닥 선택은 0번, 고/스톱은 STOP을 선택합니다.

타이머는 `TurnStep(round, turn, phase)`를 비교해 낡은 예약이 최신 예약을 덮어쓰지 못하게 합니다. 같은 단계에서 연속 바닥 선택이 발생할 수 있어 **동일 단계의 재등록은 허용**합니다. 교체 판단은 원자적으로 수행하고 대기 작업 취소는 그 밖에서 수행합니다.

대기 타이머와 이미 발사한 실행 구독은 수명을 분리합니다. 발사 시 최신 상태를 재검증하고, 사용자 요청과 경합하면 현재 타이머·방 수명을 확인해 1초 뒤 재확인합니다. 방 정리·교체 시 예약과 관리 구독을 회수합니다.

END 재시작은 수락한 실행 안에서 세션을 보존하고 다음 판을 준비합니다. 전체 정리는 세션 매핑까지 제거합니다. 이전 실행이 같은 roomId로 재생성된 방을 수정하지 않도록 실행 게이트·타이머 수명·세션 identity를 각각 확인합니다.

근거: [AutoPlayScheduler](../game-service/src/main/java/com/pomingmatgo/gameservice/infrastructure/scheduler/AutoPlayScheduler.java) · [RoomTimerLifecycle](../game-service/src/main/java/com/pomingmatgo/gameservice/infrastructure/scheduler/RoomTimerLifecycle.java) · [RoomCleanupService](../game-service/src/main/java/com/pomingmatgo/gameservice/application/room/RoomCleanupService.java)

## 재접속과 송신 순서

행동 대기 phase이고 상대가 접속 중이면 이탈자의 방과 타이머를 유지합니다. `CONNECT` 재등록 시 `RECONNECT_STATE`로 손패·바닥·획득 카드·점수·선택지·남은 시간을 전달합니다. 낡은 disconnect가 새 연결을 제거하지 않도록 세션 identity를 검사하며, 마지막 접속자가 나가면 전체 정리합니다.

1. 스냅샷의 상태·카드 조회와 DTO 조립을 공통 게임 락으로 보호합니다.
2. 액션 완료 시 수신자를 고정합니다. 스냅샷 조회 전에 완료한 액션은 스냅샷에 이미 포함되므로 그 등록에 중복 안내하지 않습니다.
3. 조회 이후 액션은 락 밖에서 해당 등록의 스냅샷 송신 성공을 기다린 뒤 안내합니다. 실패·취소·교체·정리는 대기를 실패로 끝냅니다.
4. 게임 액션·READY/UNREADY·START·선 선택/분배 안내 묶음은 락 안에서 예약한 상태 완료 순서를 따릅니다. 예약은 정상·오류·취소 때 회수합니다.

현재는 메시지별 `send`와 안내 예약을 유지하며 세션별 프레임 큐를 도입하지 않았습니다. 일반 송신의 반환 Mono는 송신 Publisher까지 기다리지만 실패·skip을 흡수하므로 **전달 성공이나 클라이언트 ACK를 뜻하지 않습니다.** 스냅샷은 실패·skip을 오류로 전달합니다.

순서 보장은 위 안내 묶음 사이의 서버 Publisher 순서입니다. 일반 CONNECT·disconnect/정리 안내와의 전체 순서, 액션 내부 병렬 안내의 전체 순서는 보장하지 않습니다. 세션 확인과 실제 송신도 원자적이지 않습니다. 느린 연결은 같은 방의 뒤 안내를 늦출 수 있고, 사용자 송신·예약 대기량과 전체 메모리 상한은 없습니다.

전체 정리는 소켓을 일괄 close하지 않습니다. 이미 시작한 송신이나 Netty·OS 버퍼의 프레임은 정리·취소 후에도 도착할 수 있습니다. 일반 안내의 자동 누락 복구와 이전 방 프레임을 새 방에 안전하게 적용하는 프로토콜은 미지원입니다. 외부 프런트엔드의 실제 적용은 별도 검증 대상입니다.

근거: [ReconnectService](../game-service/src/main/java/com/pomingmatgo/gameservice/application/connection/ReconnectService.java) · [ActionNotificationScope](../game-service/src/main/java/com/pomingmatgo/gameservice/infrastructure/messaging/ActionNotificationScope.java) · [ActionNotificationOrder](../game-service/src/main/java/com/pomingmatgo/gameservice/infrastructure/session/ActionNotificationOrder.java) · [MessageSender](../game-service/src/main/java/com/pomingmatgo/gameservice/infrastructure/messaging/MessageSender.java)

## 설계 선택과 트러블슈팅

턴제 게임은 입력을 기다리는 시간이 길어 WebFlux의 비동기 I/O로 다수 연결을 처리합니다. 기본 저장소는 Redis에서 `ConcurrentHashMap`으로 전환해 상태 접근의 네트워크 왕복·직렬화 비용을 줄였으며, Redis는 선택 프로파일로 유지합니다. 전환 자체의 성능 개선율을 주장하는 동일 조건 비교는 없습니다.

| 문제 | 해결 |
| --- | --- |
| 만료된 InFlight가 새 요청을 차단하거나 이전 요청이 새 소유자의 플래그를 삭제 | 만료 엔트리 원자적 교체 + 소유 토큰 검증 후 해제 |
| 정리와 저장의 경합으로 삭제된 GameState가 다시 생성 | in-memory `computeIfPresent`, Redis `setIfPresent`로 갱신. 실행·정리·재생성의 조정은 별도 게이트가 담당 |
| 타이머 취소가 발사한 게임 처리·종료 안내까지 끊음 | 대기 예약과 발사한 실행 구독을 분리 |
| 세션 등록 전 수신자 조회로 첫 브로드캐스트 유실 | 일반 송신은 `Flux.defer`로 구독 시점에 조회. 액션 안내는 완료 경계의 수신자 캡처를 사용 |
| 저장 후 송신 취소로 다음 타이머 누락 | 필수 후처리를 게임 락 안으로 이동하고 수락 실행이 소유 |
| 다음 액션 안내가 이전 안내 또는 재접속 스냅샷보다 먼저 도착 | 상태 완료 시 안내 순서 예약 + 등록별 스냅샷 완료 대기 |

바닥·획득 카드는 저장소 조회 시 `Card` natural order로 정렬해 선택 인덱스와 피 뺏기가 Set 순회 순서에 좌우되지 않게 합니다. 손패·덱은 순서가 게임 의미를 가지므로 유지합니다. [DeterminismReplayTest](../game-service/src/test/java/com/pomingmatgo/gameservice/application/game/DeterminismReplayTest.java)는 고정 덱·동일 명령을 다른 방에서 재실행하고 게임 상태·손패·획득 카드·바닥·잔여 덱을 비교합니다. 이는 재현성 검증이며 영속 저장이나 재시작 복구 기능은 아닙니다.

## 코드 찾아보기

소스 루트: [`game-service/src/main/java/com/pomingmatgo/gameservice`](../game-service/src/main/java/com/pomingmatgo/gameservice)

| 패키지 | 책임 |
| --- | --- |
| `api` | HTTP·WebSocket 수신, 디코딩·라우팅 |
| `application/room` | 방 생성·입퇴장·준비·정리 |
| `application/pregame` | 선 선택·분배·첫 턴 준비 |
| `application/game` | 게임 액션·실행 수락·완료·턴 진행·안내 조율 |
| `application/connection` | 접속·이탈·재접속 스냅샷 |
| `domain`, `domain/card` | 게임·플레이어 상태, 카드 모델 |
| `domain/rule`, `domain/score` | 카드 매칭, 점수·정산 |
| `domain/repository` | 저장소 인터페이스 |
| `domain/messaging`, `domain/event` | 응답 모델, 실행 실패·방 정리 이벤트 |
| `infrastructure/repository/{inmemory,redis}` | 프로파일별 저장소 |
| `infrastructure/lock`, `infrastructure/session` | 락·InFlight·실행 게이트, 세션·스냅샷·안내 순서 |
| `infrastructure/messaging`, `infrastructure/scheduler` | 송신과 자동플레이 |
| `global` | 설정·예외·응답 래퍼·계측 |

테스트는 대상 역할의 패키지에, 여러 기능을 묶는 게임 흐름 테스트는 `application/game`에 둡니다. 이는 책임별 코드 배치이며 계층 의존성을 강제하는 구조는 아닙니다. 응답 DTO 위치와 게임 규칙의 응답 이벤트 참조는 기존 설계를 유지합니다.
