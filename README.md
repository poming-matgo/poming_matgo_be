# Poming Matgo

### 10,000개의 연결, 방마다 일관된 게임 흐름.

**Java 21 · Spring WebFlux · WebSocket으로 만든 1:1 실시간 턴제 맞고 서버.**

카드 한 장을 내는 요청부터 자동플레이, 재접속, 다음 판까지 이어지는 서버입니다. 단일 인스턴스에서 다수의 연결을 처리하면서 **요청 취소와 느린 송신에도 수락한 게임 처리를 끝내는 것**에 집중했습니다.

| 동시 WebSocket | 평균 서버 송신 | 완주 게임 |
| :---: | :---: | :---: |
| **10,000 연결** | **86,454 msg/s** | **224,758판** |
| 5,000방 × 2명 | 유지 구간 내부 400개 샘플 | PLAYER_1의 GAME_OVER 기준 |

2026-10-03 in-memory 측정. 서버와 k6를 같은 머신에서 실행한 관측값이며, 송신은 서버 Publisher 완료 기준입니다. [측정 조건과 해석](docs/performance.md)을 함께 확인하세요.

[설계](#설계) · [빠른 시작](#빠른-시작) · [검증](#검증) · [지원 범위](#지원-범위)

<details>
<summary>게임 화면 보기</summary>

<img width="1015" height="533" alt="웹 고스톱 게임 플레이 화면" src="https://github.com/user-attachments/assets/0cbac32e-dd16-4f6c-abab-baf98d026d18" />

</details>

## 설계

**상태 변경은 방 단위로 보호하고, 네트워크 송신은 락 밖에서 기다립니다.**

사용자 입력과 자동플레이는 같은 게임 처리 경로를 사용합니다. 최신 상태 검증, 저장, 다음 타이머 등록 또는 종료 후 재시작을 먼저 끝내고, 해당 상태에서 확정한 수신자와 순서에 따라 안내를 보냅니다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/architecture-dark.svg">
  <img src="docs/architecture-light.svg" width="1000" alt="인메모리 동시성 제어 아키텍처. 사용자 게임 액션과 자동플레이는 InFlight 제어와 TurnFlowService를 공유한다. 공통 GameLock의 executor는 수락한 실행을 호출자 취소와 분리하고, 실행 게이트는 방 정리·재생성을 조정한다. 락 안에서 수신자와 안내 순서를 확정하며, 락 밖에서는 ActionNotificationOrder와 SnapshotDelivery로 이전 안내·재접속 스냅샷 송신 완료를 기다린 뒤 메시지를 보낸다.">
</picture>

그림은 주요 호출 관계와 **수락한 실행 보호·정리/재생성 조정·안내 순서/재접속 동기화**를 보여줍니다. 파란 영역은 공통 락의 보호 범위, 점선은 락 안에서 확정한 수신자·안내 순서의 전달입니다. 중간 핸들러와 재접속·정리의 전체 호출 경로는 생략했으며, 상세 보장 범위는 아래에 정리했습니다.

| 설계 과제 | 해결 방식 |
| --- | --- |
| 같은 방의 요청·자동플레이 경합 | InFlight 중복 제어 + 공통 `@GameLock` + 락 안의 최신 phase·플레이어 검증. 경합은 `TRY_AGAIN`으로 즉시 거절 |
| 연결 종료로 상태가 일부만 변경 | 검증 후 첫 변경 전에 실행을 수락. in-memory executor가 호출자 취소와 필수 저장·후처리를 분리 |
| 느린 송신 때문에 다음 턴이 멈춤 | 저장·타이머·END 재시작은 락 안에서 완료. 안내 묶음은 락 밖에서 상태 완료 순서대로 송신 |
| 재접속 스냅샷과 실시간 안내의 역전·중복 | 락으로 스냅샷 조회를 보호. 조회 전 액션은 스냅샷에 포함하고, 이후 액션 안내는 스냅샷 송신 완료를 기다림 |
| 방 정리와 진행 중 실행의 충돌 | 실행 게이트가 새 액션을 차단하고 기존 실행을 기다림. 중복 정리를 병합하고 새 방 생성과 조정 |
| 입력하지 않는 플레이어 | 카드 제출·바닥 선택·고/스톱에 자동플레이 적용. 10초 제한 + 2초 유예, 단조 시간으로 기한 계산 |
| 같은 입력의 결과가 컬렉션 순서에 의존 | 바닥·획득 카드를 정렬하고 손패·덱 순서는 유지. 고정 덱·명령 재실행으로 최종 상태 비교 |

안내 순서 보호는 게임 액션·준비·선 선택/분배 묶음에 적용됩니다. 느린 연결은 같은 방의 후속 안내를 늦출 수 있습니다. [실행·송신·정리 계약과 트러블슈팅 →](docs/design.md)

### 기술 구성

| 영역 | 구성 |
| --- | --- |
| 런타임 | Java 21 · Spring Boot 3.3.5 · WebFlux / Reactor · WebSocket |
| 저장소·동시성 | 기본: ConcurrentHashMap + 방별 실행 게이트 / 선택: Redis + Redisson 3.52.0 |
| 검증 | JUnit 5 · reactor-test · 실제 WebSocket/TCP 통합 테스트 · Node 클라이언트 제어 테스트 |
| 부하·관측 | k6 · 서버 송신 계측 · 선택적 InfluxDB / Grafana |

활성 모듈은 **`game-service` 하나**입니다. 초기 MSA 모듈(`config-server`, `api-gateway`, `user-service`, `auth-service`)은 빌드에서 제외했습니다. [패키지별 책임](docs/design.md#코드-찾아보기)

## 빠른 시작

JDK 21과 Gradle 8.5를 준비합니다. Java toolchain 자동 다운로드 설정은 없습니다. Wrapper는 Git에 포함되지 않으므로 새 clone에서는 한 번 생성합니다. 아래 명령은 Linux/WSL 기준이며, Windows에서는 `./gradlew`를 `.\gradlew.bat`으로 바꿉니다.

```bash
# 새 clone에서 wrapper 준비
gradle wrapper --gradle-version 8.5

# Redis 없이 실행
./gradlew :game-service:bootRun --args='--spring.profiles.active=in-memory --spring.autoconfigure.exclude=org.redisson.spring.starter.RedissonAutoConfigurationV2,org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration'
```

기본 프로파일은 `in-memory`입니다. Redisson starter가 Redis 연결을 시도할 수 있어 위 명령에서 자동 설정을 제외합니다.

| 접점 | 주소 |
| --- | --- |
| 방 생성·입장 | `POST http://localhost:8084/room` · `POST /room/join` |
| WebSocket | `ws://localhost:8084/gostop` |
| 송신 계측 | `GET http://localhost:8084/internal/metrics/throughput` |

<details>
<summary>Redis 프로파일과 계측 옵션</summary>

Redis 서버를 준비한 뒤 실행합니다. `application-dev.properties`는 `redis` 프로파일만 활성화하면 적용되지 않으므로 접속 정보를 명시합니다.

```bash
./gradlew :game-service:bootRun --args='--spring.profiles.active=redis --spring.data.redis.host=localhost --spring.data.redis.port=6379'
```

송신 계측은 기본 활성화됩니다. `--metrics.throughput.enabled=false`로 빈과 엔드포인트를 끄거나, `DELETE /internal/metrics/throughput`으로 집계를 초기화할 수 있습니다. [`loadtest/docker-compose.yml`](loadtest/docker-compose.yml)은 InfluxDB/Grafana만 실행합니다.

</details>

### 한 판의 흐름

1. HTTP로 방을 생성하고 두 사용자가 각각 입장합니다.
2. 각자 WebSocket 연결 후 `CONNECT`를 보내고, 자신의 연결 응답을 확인합니다.
3. 양쪽 `READY` → 양쪽 `START` 수신 → `LEADER_SELECTION` → 카드 분배·첫 턴으로 진행합니다.
4. 서버 안내에 따라 `NORMAL_SUBMIT` · `FLOOR_SELECT` · `GO_STOP_CHOICE`를 보냅니다. 입력이 없으면 자동플레이가 이어갑니다.
5. 양쪽 `GAME_OVER` 수신 후 다시 `READY`를 보내 다음 판을 시작합니다.

행동 대기 중 이탈해도 상대가 접속해 있으면 방을 보존합니다. 돌아온 사용자는 기존 `CONNECT`와 `RECONNECT_STATE`로 손패·바닥·획득 카드·점수·선택지·남은 시간을 동기화합니다. 마지막 접속자가 이탈하면 방을 정리합니다.

요청 JSON과 응답 처리 예시는 [`gostop-test.js`](gostop-test.js), 양쪽 연결·준비 순서 제어는 [`loadtest/room-run.js`](loadtest/room-run.js)에 있습니다.

## 검증

```bash
./gradlew :game-service:test                  # 모듈 전체 테스트
node --test loadtest/client-lifecycle.test.cjs # k6 클라이언트 제어 로직
./gradlew :game-service:build                 # 테스트 포함 빌드
```

테스트 보고서: `game-service/build/reports/tests/test/index.html`

| 검증 기록 | 결과 | 확인한 범위 |
| --- | --- | --- |
| 서버 회귀 · 2026-10-01 | **488개 통과 / 60개 클래스** | 게임 규칙·결정성·경합·취소·재접속·정리 |
| 실제 소켓 · 위 회귀에 포함 | **24개 통과 / 7개 클래스** | 실제 WS 진행, 느린 수신, 송신·종료·회수의 제한된 조건 |
| 클라이언트 제어 · 2026-10-01 | **25개 통과** | 연결·준비 장벽, 중복 요청 방지, 오류 종료·예약 회수 |
| 본 부하 · 2026-10-03 | **5,000방 / 모든 threshold 통과** | 224,758판, 액션 RTT P95 81ms · P99 149ms 관측 |

위 수치는 기존 실행 기록입니다. RTT는 요청 이후 첫 정상 메시지까지의 시간이며, 측정 중 시스템 시계 불연속이 관측되었습니다. 종료 시 VU 취소 로그와 카운터 차이도 함께 기록했습니다. [최신·과거 결과와 측정 한계 →](docs/performance.md)

<details>
<summary>영역별 회귀 · k6 실행 명령</summary>

```bash
# 동시성·자동플레이·재접속
./gradlew :game-service:test --tests '*ConcurrentGameActionTest' --tests '*AutoPlay*Test' --tests '*DisconnectReconnectTest'

# 카드 순서·결정성
./gradlew :game-service:test --tests '*CanonicalCardOrderTest' --tests '*DeterminismReplayTest'

# 서버 실행 후: 1방 2인이 입력 없이 자동플레이로 완주 (약 4~8분)
k6 run gostop-afk-test.js

# 대규모 부하: 2분 증가 → 5,000방 7분 유지 → 1분 감소
k6 run gostop-test.js
```

k6 설치가 필요합니다. 기본 대상은 `127.0.0.1:8084`이며, 다른 서버에는 `-e BASE_HTTP_URL=http://127.0.0.1:18084 -e BASE_WS_URL=ws://127.0.0.1:18084/gostop`을 함께 지정합니다. 일반 부하는 방 생성·입장·실행 실패 시 전체 실행을 중단합니다.

</details>

## 지원 범위

**현재 검증의 중심은 단일 인스턴스의 in-memory 프로파일입니다.**

- **실행 중 재접속 지원.** 영속 로그·스냅샷 저장·프로세스 장애 복구는 미구현이며, 인메모리 상태는 프로세스 종료 시 소실됩니다.
- **Redis는 선택 구현.** 세션·타이머는 인스턴스 로컬이므로 다중 인스턴스에는 방 라우팅·소유권·복구 설계가 필요합니다. Redis 런타임의 취소 안전성은 별도 검증 대상입니다.
- **송신 보장은 명시된 안내 묶음까지.** 모든 이벤트의 전체 순서, 누락 자동 복구, 이전 방 프레임의 새 방 적용 안전성, 송신 대기량·메모리 상한은 지원하지 않습니다.
- **총통은 감지만 구현.** 한쪽·양쪽 총통의 승부 처리는 미구현이며 현재는 첫 턴으로 진행합니다.
