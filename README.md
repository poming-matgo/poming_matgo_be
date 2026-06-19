<img width="1015" height="533" alt="Animation" src="https://github.com/user-attachments/assets/0cbac32e-dd16-4f6c-abab-baf98d026d18" />

# 🎴 웹 고스톱 게임 (Web Go-Stop)

> **대규모 동시 접속 환경을 고려한 실시간 턴제 웹 고스톱 게임입니다.**
> 불필요한 네트워크/스레드 오버헤드를 줄여 **초당 약 68,000건의 웹소켓 메시지를 지연 없이 처리**하도록 최적화했습니다.

<br/>

## 🛠 기술 스택
- Java 21, Spring WebFlux, WebSocket, Redis *(분산 프로파일)*
- `ConcurrentHashMap` 기반 In-Memory *(기본 프로파일)*

<br/>

## 🏗 아키텍처

게임 도메인 특성상 **단일 서비스 (`game-service`)** 가 모든 게임 로직과 동시성 제어를 담당합니다. 이벤트는 카테고리별 핸들러로 분기되고, 각 단계에 맞는 동시성 레이어가 적용됩니다.

```text
Client ──WS──▶ GameWebSocketHandler
                     │
       ┌─────────────┼─────────────────────────────────────────┐
       │             │                                         │
       │             │      1) InFlightManager (Game 액션 경로에만 적용:
       │             │         자동플레이 ↔ 정상 요청 race 1차 fail-fast,
       │             │         유저제출 / 자동플레이 키 분리)
       │             │                                          │
       ▼             ▼                                          ▼
  WsRoomHandler  WsPreGameHandler                         WsGameHandler
       │               │                                       │
       ▼               ▼                                       ▼
  RoomService      PreGameService                       GamePlayService ◀─── 재진입 ───────┐
       │               │                                       │                          │
       ▼               ▼                                       ▼                          │
  2) RoomLockManager  2) Atomic Trigger                  2) @GameLock AOP                 │
   (방 단위 Semaphore, (LeadingPlayer,                    (round:turn 단위 Semaphore,       │
    Ready / Join 등    putIfAbsent로                      자동플레이 ↔ 정상 요청              │
    직렬화)            선플레이어 결정                     race final guard)                 │
       │               1회 보장)                               │                           │ 
       │               │                                       │                          │
       └───────────────┴───────────────┬───────────────────────┘                          │
                                       ▼                                                  │
                    ConcurrentHashMap (In-Memory Game State) ── 등록 ──────────────────────┤
                                                                                          │
                         3) AutoPlayScheduler ─── 타이머 발사 (Game 액션) ──────────────────┘
```

**동시성 3레이어 요약:**

- **① In-Flight 플래그** — **자동플레이(③)와 정상 요청 사이의 race 1차 fail-fast** 가 본 목적. 같은 플레이어 단위로 `NORMAL` / `AUTOPLAY` 키를 분리해, 자동플레이는 `isSet(NORMAL)`로 양보하고 정상 요청은 자동플레이 진행 중에도 In-Flight 단계에서 막히지 않음. TTL 만료 엔트리는 `ConcurrentHashMap.replace(k, old, new)` 기반 원자적 비교-교체 loop로 자동 갱신. *(InFlight 통과 후의 race window는 `@GameLock`(②)이 final guard, 두 플레이어 동시 액션은 `currentPlayer` 체크로 차단 — 책임 분리)*
- **② 직렬화 / atomic 제어 (역할 분리)** — **공유 객체 수정에는 락, 데이터가 player별로 분리된 곳에는 atomic 연산**으로 메커니즘 비용 최소화. 핸들러별로 본 목적이 다름:
  - **`RoomLockManager`** (방 단위 Semaphore): 단일 `GameState` 객체를 공유 수정하는 Ready/Join 작업의 직렬화
  - **`@GameLock` AOP** (round:turn 단위 Semaphore): InFlight를 통과한 자동플레이 ↔ 정상 요청 race의 final guard
  - **`LeadingPlayer.tryClaimLeaderSelectionTrigger`** (`putIfAbsent` 기반 atomic): 데이터가 이미 player별로 분리되어 락이 불필요한 대신, 두 플레이어가 거의 동시에 선플레이어 카드를 선택해도 후속 처리 트리거가 1회만 발생되도록 atomic으로 보장
- **③ AutoPlay 스케줄러** — 턴 타임아웃 시 자동으로 Game 액션 발사 (현재는 `NORMAL_SUBMIT`, 추후 `FLOOR_SELECT` / `GO_STOP_CHOICE`로 확장 예정). (round, turn) 시퀀스를 단조 증가시켜 동시 호출의 순서와 무관하게 가장 큰 sequence(=최신 턴)의 task만 살아남도록 atomic swap. 발사 시 `isSet(NORMAL)` 체크로 정상 요청에게 양보하고, 자체 동시 시작은 `InFlightManager(AUTOPLAY 키)`로 방지

> **MSA → 단일 서비스 통합:** 초기에는 `api-gateway` / `user-service` / `auth-service` / `game-service` 의 MSA로 기획했으나, **`game-service`의 동시성/성능 최적화에 집중**하기 위해 나머지 서비스는 폐기하고 단일 서비스로 통합했습니다.

<br/>

## 🚀 Getting Started

### Prerequisites

- Java 21 (Gradle toolchain이 자동 설치)
- (선택) Redis 7.x — 분산 프로파일 사용 시

### 실행

```bash
# 기본 (in-memory)
./gradlew :game-service:bootRun

# Redis 분산 프로파일
./gradlew :game-service:bootRun --args='--spring.profiles.active=redis'
```

### 부하 테스트

```bash
# k6 설치 후
k6 run gostop-test.js
```

### 부하 테스트 시계열 모니터링 (선택)

InfluxDB + Grafana 스택을 Docker로 띄워 실시간 throughput을 시계열로 측정할 수 있습니다.

```bash
# 1. 스택 기동 (Docker Desktop 필요)
cd loadtest
docker compose up -d

# 2. k6를 InfluxDB output과 함께 실행 (프로젝트 루트에서)
cd ..
k6 run --out influxdb=http://localhost:8086/k6 gostop-test.js

# 3. Grafana 접속 (대시보드 자동 provision)
# http://localhost:3000  →  "k6 Load Testing Results" 대시보드
```

<br/>

## 💡 기술 스택 선택 이유 및 아키텍처 의사결정

### 1. Spring WebFlux & WebSocket : 턴제 게임의 긴 Idle Time 극복

턴제 게임은 유저가 고민하는 동안 서버 리소스를 거의 쓰지 않는 대기 시간(Idle Time)이 깁니다. Spring MVC는 WebSocket 연결당 1개의 스레드를 점유하는 구조라 동시 접속자가 늘수록 스레드가 낭비됩니다.

이를 해결하기 위해 **I/O 이벤트가 발생할 때만 스레드가 관여하는 Event-Driven 방식의 WebFlux를 채택**하여, 적은 스레드로도 대규모 동시 접속을 효율적으로 처리하도록 구성했습니다.

### 2. In-Memory 전환 (Redis ➡️ ConcurrentHashMap) : 처리량 극대화

초기에는 게임 상태 관리를 위해 Redis를 도입했으나, **단일 방 안에서만 상태 공유가 필요한 게임 도메인 특성**상 분산 캐시의 오버헤드가 불필요하다고 판단했습니다.

`ConcurrentHashMap` 기반의 인메모리 구조로 전환하여 **TCP 통신 및 직렬화/역직렬화 비용을 완전히 제거**했습니다. *(Redis 프로파일은 그대로 유지해 분산 배포 시 전환 가능)*

- **결과:** I/O 병목을 해소하여 **초당 약 68,000건의 웹소켓 메시지를 유실 없이 처리**하며, 단일 서버 처리량이 기존 Redis 대비 **약 6.6배 향상**되었습니다.

<br/>

## 🚨 주요 트러블슈팅 및 성능 최적화

### 1. WebFlux 환경에서의 Non-blocking Lock 설계

- **문제:** 단일 방 테스트에서는 정상이었으나, 다수의 방을 동시 플레이하는 부하 테스트에서 동시성 이슈가 발생했습니다. MVC 방식대로 `synchronized` 락을 적용했으나 문제가 해결되지 않고 오히려 병목이 심화되었습니다.
- **원인:** WebFlux는 소수의 EventLoop 스레드가 다수 요청을 비동기 처리합니다. 스레드 기반 블로킹 락은 EventLoop 자체를 멈추게 만들어, **락 대기가 아닌 다른 요청까지 처리 못 하는 기아 상태**를 유발합니다.
- **해결:** `ConcurrentHashMap` + `Semaphore` 조합으로 방 단위 락을 구현하되, EventLoop와 격리되도록 설계했습니다.
  1. **방 단위 격리:** `computeIfAbsent`로 방마다 독립된 `Semaphore(1)`을 할당해 락 경합 범위 최소화
  2. **EventLoop 보호:** `tryAcquire`는 블로킹 호출이므로 `Schedulers.boundedElastic()`로 분리, EventLoop가 멈추지 않도록 격리
  3. **자원 해제 보장:** `Mono.usingWhen`으로 정상 종료/에러/구독 취소 모든 경로에서 `semaphore.release()`가 호출되도록 보장하여 락 누수 차단

### 2. 운영 시나리오 기반 동시성 정합성 강화

초기 설계 이후, 운영/장기 부하 시나리오를 점검하며 발견한 미세한 race condition을 추가로 보완했습니다.

- **In-Flight 플래그의 만료 처리:** TTL이 지났지만 잔존하는 stale 엔트리가 새 요청을 영구 차단하던 문제를, **`ConcurrentHashMap.replace(k, old, new)` 기반 원자적 비교-교체 loop**로 안전하게 갱신하도록 개선
- **자동플레이 스케줄링의 원자성:** sequence 비교 → cancel → put이 분리되어 발생하던 race를 `ConcurrentHashMap.compute` 기반 **single-record atomic swap**으로 해결. 두 동시 호출의 순서와 무관하게 더 큰 sequence의 task가 살아남도록 보장
- **락 cleanup의 안전성:** 락 보유 중인 방을 즉시 제거하면 후속 요청이 새 Semaphore를 만들어 동시 진입하는 race를, `tryAcquire` 성공 시에만 제거하도록 변경
- **정상 요청 ↔ 자동플레이 우선순위 충돌 해소:** 동일한 In-Flight 키를 공유해 자동플레이가 deadline 임박 시점에 락을 먼저 잡으면 정상 요청이 `TOO_MANY_REQUESTS`로 거부되던 race를, **키를 `NORMAL` / `AUTOPLAY` prefix로 분리**해 정상 요청이 In-Flight 단계에서 차단되지 않도록 변경. 자동플레이는 진입 시점 + 게임 로직 직전 두 단계로 `isSet(NORMAL)` 체크하여 양보하고, 정상 요청은 `@GameLock` 획득 콜백에서 `cancelAutoPlay`를 호출해 진행 중인 자동플레이를 abort. **InFlight 통과 후의 잔존 race window는 `@GameLock`이 final guard로 받아 layered defense 완성**

### 3. 네트워크 지연 및 OS 시간 역전을 고려한 타임아웃 정밀도 향상

- **문제:** 클라이언트-서버 간 네트워크 지연(Latency) 및 타이머 오차로 인해, 유저 입장에서는 턴 시간이 남았음에도 서버에서 타임아웃 처리되는 불일치가 발생했습니다.
- **해결 1 (네트워크 지연 보정):** 서버 측 타임아웃 계산 시 **유예 시간** 을 도입하여 RTT 차이를 극복
- **해결 2 (Monotonic Clock 도입):** NTP 동기화 등으로 인한 시스템 시간 역전에 대비하여, `System.currentTimeMillis()` 대신 **단조 증가 시계인 `System.nanoTime()`** 을 사용하여 타임아웃 계산의 신뢰성 확보

<br/>

## 📊 부하 테스트 결과

### 환경

- **하드웨어:** Intel i7-14700 (20 코어 / 28 스레드), RAM 32GB, Windows
- **JVM:** Java 21 (G1GC, default)
- **부하 도구:** [k6](https://k6.io/) v1.7.1 — 시나리오 코드: [`gostop-test.js`](gostop-test.js)
- **시계열 모니터링:** InfluxDB 1.8 + Grafana 10.4 (Docker, 셋업 [`loadtest/`](loadtest/))

### 시나리오

- 동시 게임 방 5,000개 × 2명 = **WebSocket 동접 10,000**
- stages: 2분 ramp up → 7분 sustain (5,000 VU) → 1분 ramp down
- 각 가상 유저는 방 생성 → 입장 → READY → 게임 진행(카드 제출/바닥 선택/GO·STOP) → 종료 후 재준비를 반복

### 결과 (대표 측정값)

| 지표 | 값 |
| :--- | :--- |
| 초당 WS 메시지 (수신, sustain 피크) | **약 68,600 msg/s** |
| 초당 WS 메시지 (수신, 전체 평균) | 약 57,000 msg/s |
| 초당 WS 메시지 (전송, 전체 평균) | 약 6,700 msg/s |
| 총 수신 메시지 | 약 36,000,000 |
| WS Handshake 시간 평균 | 약 11ms |
| WS Handshake 시간 P95 | 약 61ms |
| WS Handshake 시간 max | 약 340ms |

> 게임 메시지 단위 RTT는 k6 native 메트릭이 없어 본 측정엔 포함되지 않았습니다. 표의 Handshake 시간은 동시 5,000 VU의 connection establishment 부하에서 측정한 값입니다.

### 측정 방법론

- **콘솔 평균 → sustain 피크 보정:** k6 콘솔의 평균값은 ramp up/down 구간이 희석한 값. 실제 실행 시간은 stages 10분 + k6 기본 graceful stop 30초 = **10.5분**으로, effective sustain 시간을 `2 × 0.5 + 7 × 1.0 + 1.5 × 0.5 = 8.75분 (525s)` 으로 보정 (ramp down 가중치 0.5 + graceful 30초 포함)하면 `36M / 525s ≈ 68,600 msg/s`로, sustain 구간 피크에 수렴.
- **Run-to-run 변동성:** 동일 환경에서 여러 차례 측정 시 평균 throughput이 약 ±5% 변동 (54k ~ 58k msg/s 평균, 보정 후 약 64k ~ 69k 피크). 표의 값은 안정적으로 재현 가능한 대표 측정값.
- **InfluxDB 시계열 측정의 한계:** 1초 단위 max를 시계열로 직접 측정하려 InfluxDB + Grafana를 도입했으나, **5,000 VU 부하에서 k6 → InfluxDB write가 backpressure로 sample의 약 1.5%만 적재**되어 정확한 시계열 max 측정엔 실패. 이는 k6 + InfluxDB 조합의 알려진 한계로, 대규모 부하 시 Prometheus remote write 등 고처리 sink가 필요. 본 프로젝트에선 Grafana 시계열은 **sustain 안정성 패턴 검증** 용도로만 활용하고, 정확한 throughput은 k6 in-memory 집계(콘솔 메트릭)를 신뢰.
