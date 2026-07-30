package com.pomingmatgo.gameservice.recovery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.TurnTiming;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.lease.RoomLeaseManager;
import com.pomingmatgo.gameservice.domain.recovery.GameRecoveryService;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.GamePlayService;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.scheduler.TurnScheduler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 2-D 복구 절차 통합 테스트 — 실물 Postgres 대상, 미기동이면 전체 skip.
 * 크래시 시뮬레이션 = 로컬 상태 wipe(프로세스 죽음의 로컬 등가물) + lease 강제 만료(시간 경과 등가물).
 * 기동 예: docker run -d --name gostop-pg-test -e POSTGRES_PASSWORD=postgres -p 15432:5432 postgres:16-alpine
 */
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude="
                + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration",
        "game.log.store=postgres",
        "game.lease.store=postgres",
        // 스캔 루프 비활성 — 테스트가 scanOnce를 직접 구동해 타이밍을 통제한다
        "game.recovery.scan-interval=0"})
@DisplayName("복구(2-D): 만료 lease 인수 → 스냅샷 restore + tail replay → 타이머 재등록 → 완주")
class PostgresGameRecoveryTest {

    private static final String URL = System.getenv().getOrDefault(
            "GAME_LOG_PG_URL", "r2dbc:postgresql://postgres:postgres@localhost:15432/postgres");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final long USER_1 = 101L;
    private static final long USER_2 = 202L;
    private static final int MAX_COMMANDS = 80;
    private static final int MAX_ATTEMPTS = 5;

    @Autowired GamePlayService gamePlayService;
    @Autowired PreGameService preGameService;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired InstalledCardRepository installedCardRepository;
    @Autowired AcquiredCardRepository acquiredCardRepository;
    @Autowired RoomCleanupService roomCleanupService;
    @Autowired GameRecoveryService recoveryService;
    @Autowired RoomLeaseManager leaseManager;
    @Autowired TurnScheduler turnScheduler;
    @Autowired DatabaseClient databaseClient;
    @Autowired ObjectMapper objectMapper;

    @BeforeAll
    static void requirePostgres() {
        assumeTrue(reachable(), "Postgres 미기동 — skip: " + URL);
    }

    @DynamicPropertySource
    static void postgresUrl(DynamicPropertyRegistry registry) {
        registry.add("game.log.postgres.url", () -> URL);
    }

    private static boolean reachable() {
        URI uri = URI.create(URL.substring("r2dbc:".length()));
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort() == -1 ? 5432 : uri.getPort()), 1500);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static long newRoomId() {
        return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    }

    private record CapturedState(String gameState, List<Card> p1Hand, List<Card> p2Hand,
                                 List<Card> p1Acquired, List<Card> p2Acquired,
                                 List<Card> floor, List<Card> hiddenDeck) {}

    @Test
    @DisplayName("중반 크래시: 스냅샷 restore + tail replay로 상태 동등 복원, seq 사슬을 이어 완주·마감까지 간다")
    void midGameCrashRecoversAndFinishes() throws JsonProcessingException {
        long roomId = newRoomId();
        int commands = startAndPlayUntilRound2(roomId);
        CapturedState beforeCrash = capture(roomId);
        awaitPersisted(roomId, commands + 1);
        await("라운드 경계 스냅샷 영속", () -> snapshotCount(roomId) >= 1);

        crash(roomId);
        forceExpire(roomId);
        // 자동플레이가 검증 중에 끼어들지 않도록 미래 deadline을 심는다 (2-C 복원 경로 검증 겸용)
        presetDeadline(roomId, System.currentTimeMillis() + 60_000);

        recoveryService.scanOnce().block(TIMEOUT);

        assertEquals(beforeCrash, capture(roomId), "복구 후 상태가 크래시 직전과 다르다");
        assertEquals(2L, currentToken(roomId), "인수는 fencing token을 올려야 한다");
        // 복원된 deadline(+60s)으로 타이머가 재등록됐다 — 타이머가 없으면 기본값(TURN_TIMEOUT)이 돌아온다
        assertTrue(turnScheduler.getRemainingTurnMillis(roomId) > TurnTiming.TURN_TIMEOUT_MILLIS,
                "복원 deadline 기반 자동플레이 타이머가 등록돼야 한다");

        // 복구 후 라이브 커맨드가 같은 세대의 seq 사슬을 이어야 한다 — 완주 후 결번/중복 없이 완료 표시까지
        int resumed = playLiveGame(roomId);
        roomCleanupService.cleanupRoomData(roomId).block(TIMEOUT);
        assertSeqChainComplete(roomId, commands + 1 + resumed);
        assertTrue(latestGenerationCompleted(roomId), "완주 게임은 완료 표시돼야 한다");
        assertEquals("released", leaseOwner(roomId), "정상 마감은 lease를 해제해야 한다");
    }

    @Test
    @DisplayName("라운드 1 크래시(스냅샷 없음): DECK_INIT의 출생 기록만으로 userIds·선플레이어까지 복원된다")
    void roundOneCrashRecoversFromDeckInitAlone() throws JsonProcessingException {
        long roomId = playSingleCommandGame(roomId());
        CapturedState beforeCrash = capture(roomId);
        awaitPersisted(roomId, 2);

        crash(roomId);
        forceExpire(roomId);
        presetDeadline(roomId, System.currentTimeMillis() + 60_000);

        recoveryService.scanOnce().block(TIMEOUT);

        assertEquals(beforeCrash, capture(roomId), "스냅샷 없는 복구 후 상태가 크래시 직전과 다르다");
        GameState restored = gameStateRepository.findById(roomId).block(TIMEOUT);
        assertEquals(USER_1, restored.getPlayerState(Player.PLAYER_1).getUserId(), "user1 복원 실패 — 재접속 불가가 된다");
        assertEquals(USER_2, restored.getPlayerState(Player.PLAYER_2).getUserId());
        assertEquals(1, restored.getLeadingPlayer());

        roomCleanupService.cleanupRoomData(roomId).block(TIMEOUT);
    }

    @Test
    @DisplayName("종료 커맨드 후·cleanup 전 크래시: 인수 노드가 완료 표시와 lease 해제만 마저 한다")
    void crashAfterFinalCommandIsClosedOut() {
        long roomId = newRoomId();
        int commands = startGame(roomId) + playLiveGame(roomId);
        awaitPersisted(roomId, commands + 1);
        assertFalse(latestGenerationCompleted(roomId), "cleanup 전이므로 아직 미완료여야 한다");

        crash(roomId);
        forceExpire(roomId);

        recoveryService.scanOnce().block(TIMEOUT);

        assertNull(gameStateRepository.findById(roomId).block(TIMEOUT), "END로 끝난 방은 복구 후 정리돼야 한다");
        assertTrue(latestGenerationCompleted(roomId), "인수 노드가 완료 표시를 마저 해야 한다");
        assertEquals("released", leaseOwner(roomId));
    }

    @Test
    @DisplayName("복원할 기록이 없는 만료 lease(DECK_INIT 전 크래시)는 인수 후 즉시 해제된다")
    void expiredLeaseWithoutGenerationIsReleased() {
        long roomId = newRoomId();
        leaseManager.acquire(roomId).block(TIMEOUT);
        forceExpire(roomId);

        recoveryService.scanOnce().block(TIMEOUT);

        assertNull(gameStateRepository.findById(roomId).block(TIMEOUT));
        assertEquals("released", leaseOwner(roomId));
    }

    @Test
    @DisplayName("레코드 없는 미완료 세대(DECK_INIT 유실 창): 마감만 하고, 같은 방의 다음 게임 기록은 정상 동작한다")
    void generationWithoutRecordsIsClosedOutAndRoomStaysUsable() {
        long roomId = newRoomId();
        leaseManager.acquire(roomId).block(TIMEOUT);
        databaseClient.sql("INSERT INTO game_generation (room_id) VALUES (:roomId)")
                .bind("roomId", roomId).then().block(TIMEOUT);
        forceExpire(roomId);

        recoveryService.scanOnce().block(TIMEOUT);

        assertTrue(latestGenerationCompleted(roomId), "복원 불가 세대는 완료로 마감해 재스캔을 멈춰야 한다");
        assertEquals("released", leaseOwner(roomId));

        // replay 억제 플래그 잔류 회귀 — 잔류하면 새 게임의 DECK_INIT이 조용히 유실된다 (startGame이 세대 발급을 대기)
        startGame(roomId);
        roomCleanupService.cleanupRoomData(roomId).block(TIMEOUT);
    }

    @Test
    @DisplayName("로컬에 살아있는 방은 lease가 만료돼도 자기-인수하지 않는다 — 진행 중 게임과의 replay 경합 차단")
    void locallyAliveRoomIsNeverSelfTakenOver() {
        long roomId = newRoomId();
        leaseManager.acquire(roomId).block(TIMEOUT);
        gameStateRepository.create(GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS).build()).block(TIMEOUT);
        forceExpire(roomId);

        recoveryService.scanOnce().block(TIMEOUT);

        assertEquals(1L, currentToken(roomId), "로컬 생존 방을 인수(token 증가)하면 안 된다");
        roomCleanupService.cleanupRoomData(roomId).block(TIMEOUT);
    }

    // ── 게임 구동 헬퍼 ──────────────────────────────────────────────────────────

    private long roomId() {
        return newRoomId();
    }

    /** lease 획득 + 방 생성 + 셔플 배분(DECK_INIT 기록) — proceedToGameStart의 최소 등가물. 반환은 실행한 커맨드 수(0) */
    private int startGame(long roomId) {
        leaseManager.acquire(roomId).block(TIMEOUT);
        GameState initial = GameState.builder()
                .roomId(roomId)
                .player1(PlayerState.builder().userId(USER_1).ready(true).build())
                .player2(PlayerState.builder().userId(USER_2).ready(true).build())
                .leadingPlayer(1)
                .currentTurn(1)
                .round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(initial).block(TIMEOUT);
        long generationsBefore = generationCount(roomId);
        preGameService.distributeCards(initial).block(TIMEOUT);
        // 테스트는 커맨드를 in-memory 속도로 몰아친다 — 세대 발급(DECK_INIT 첫 배치 flush) 전에 라운드 경계에 도달하면
        // 스냅샷이 세대 미해소로 버려진다(유실 허용 계약). 실서비스 cadence엔 없는 아티팩트라 여기서 발급을 기다린다
        await("게임 세대 발급", () -> generationCount(roomId) == generationsBefore + 1);
        return 0;
    }

    private long generationCount(long roomId) {
        return databaseClient.sql("SELECT count(*) AS cnt FROM game_generation WHERE room_id = :roomId")
                .bind("roomId", roomId)
                .map(row -> row.get("cnt", Long.class))
                .one().block(TIMEOUT);
    }

    /** 라운드 2 진입까지 진행하고 행동 대기 상태에서 멈춘다 — 드물게 라운드 1에서 끝나면 새 방으로 재시도 */
    private int startAndPlayUntilRound2(long roomId) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            startGame(roomId);
            int commands = 0;
            GameState state = gameStateRepository.findById(roomId).block(TIMEOUT);
            while (state.getRound() < 2 && state.getPhase() != GamePhase.END && commands < MAX_COMMANDS) {
                executeOne(roomId, state);
                commands++;
                state = gameStateRepository.findById(roomId).block(TIMEOUT);
            }
            if (state.getRound() >= 2 && state.getPhase().isPlayerActionPhase()) {
                return commands;
            }
            roomCleanupService.cleanupRoomData(roomId).block(TIMEOUT);
        }
        return fail(MAX_ATTEMPTS + "번 연속 라운드 2 진입 실패 — 통계적으로 비정상");
    }

    /** 커맨드 1개만 실행된 라운드 1 상태를 만든다 — 첫 커맨드로 끝나는 극단 셔플이면 새 방으로 재시도 */
    private long playSingleCommandGame(long roomId) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            startGame(roomId);
            GameState state = gameStateRepository.findById(roomId).block(TIMEOUT);
            executeOne(roomId, state);
            state = gameStateRepository.findById(roomId).block(TIMEOUT);
            if (state.getPhase().isPlayerActionPhase()) {
                return roomId;
            }
            roomCleanupService.cleanupRoomData(roomId).block(TIMEOUT);
            roomId = newRoomId();
        }
        return fail(MAX_ATTEMPTS + "번 연속 첫 커맨드 종료 — 통계적으로 비정상");
    }

    /** 상태를 보고 커맨드를 결정·실행 — 자동플레이와 같은 정책. 실행한 커맨드 수 반환 */
    private int playLiveGame(long roomId) {
        for (int count = 0; count < MAX_COMMANDS; count++) {
            GameState state = gameStateRepository.findById(roomId).block(TIMEOUT);
            if (state.getPhase() == GamePhase.END) {
                return count;
            }
            executeOne(roomId, state);
        }
        return fail("게임이 " + MAX_COMMANDS + " 커맨드 안에 완주되지 않았다");
    }

    private void executeOne(long roomId, GameState state) {
        switch (state.getPhase()) {
            case IN_PROGRESS ->
                    gamePlayService.executeNormalSubmit(roomId, state.getCurrentPlayer(), 0, null).block(TIMEOUT);
            case AWAITING_FLOOR_CARD_CHOICE ->
                    gamePlayService.executeFloorSelection(roomId, state.getChoiceInfo().getPlayerNumToChoose(), 0, null).block(TIMEOUT);
            case AWAITING_GO_STOP_CHOICE -> {
                Player actor = state.getCurrentPlayer();
                gamePlayService.executeGoStop(roomId, actor, state.getPlayerState(actor).getGo() == 0, null).block(TIMEOUT);
            }
            default -> fail("예상 밖 phase: " + state.getPhase());
        }
    }

    // ── 크래시 시뮬레이션·검증 헬퍼 ─────────────────────────────────────────────

    /** 프로세스 죽음의 로컬 등가물 — durable 저장소(로그·스냅샷·lease 행)는 남기고 인메모리 상태만 지운다 */
    private void crash(long roomId) {
        gameStateRepository.cleanup(roomId).block(TIMEOUT);
        installedCardRepository.cleanup(roomId).block(TIMEOUT);
        acquiredCardRepository.cleanup(roomId).block(TIMEOUT);
    }

    private void forceExpire(long roomId) {
        databaseClient.sql("UPDATE room_lease SET expires_at = now() - interval '1 second' WHERE room_id = :roomId")
                .bind("roomId", roomId).then().block(TIMEOUT);
    }

    private void presetDeadline(long roomId, long deadlineEpochMillis) {
        databaseClient.sql("UPDATE room_lease SET turn_deadline_epoch_millis = :deadline WHERE room_id = :roomId")
                .bind("deadline", deadlineEpochMillis)
                .bind("roomId", roomId).then().block(TIMEOUT);
    }

    private CapturedState capture(long roomId) throws JsonProcessingException {
        GameState state = gameStateRepository.findById(roomId).block(TIMEOUT);
        assertNotNull(state, "대상 방의 게임 상태가 없다");
        return new CapturedState(
                objectMapper.writeValueAsString(state),
                installedCardRepository.getPlayerCards(roomId, Player.PLAYER_1).block(TIMEOUT),
                installedCardRepository.getPlayerCards(roomId, Player.PLAYER_2).block(TIMEOUT),
                acquiredCardRepository.getAllCards(roomId, 1).block(TIMEOUT),
                acquiredCardRepository.getAllCards(roomId, 2).block(TIMEOUT),
                installedCardRepository.getAllRevealedCards(roomId).block(TIMEOUT),
                installedCardRepository.getHiddenCards(roomId).block(TIMEOUT));
    }

    /** 로그 writer는 락 밖 비동기 배치 — 크래시 시뮬레이션 전에 durable 반영을 기다린다 (실제 크래시라면 이 갭이 곧 RPO) */
    private void awaitPersisted(long roomId, long expectedRecords) {
        await("게임 로그 " + expectedRecords + "건 영속", () -> logCount(roomId) >= expectedRecords);
    }

    private void await(String what, BooleanSupplier condition) {
        for (int i = 0; i < 200 && !condition.getAsBoolean(); i++) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(e);
            }
        }
        assertTrue(condition.getAsBoolean(), what);
    }

    private void assertSeqChainComplete(long roomId, long expectedRecords) {
        List<Long> seqs = databaseClient.sql("""
                        SELECT seq FROM game_log
                        WHERE room_id = :roomId
                          AND game_id = (SELECT max(game_id) FROM game_generation WHERE room_id = :roomId)
                        ORDER BY seq
                        """)
                .bind("roomId", roomId)
                .map(row -> row.get("seq", Long.class))
                .all().collectList().block(TIMEOUT);
        assertEquals(expectedRecords, seqs.size(), "레코드 수 = 크래시 전 + 재개 후 커맨드 합");
        for (int i = 0; i < seqs.size(); i++) {
            assertEquals(i + 1, seqs.get(i), "seq 결번/중복 — 복구 후 seq 재개(seed)가 깨졌다");
        }
    }

    private long logCount(long roomId) {
        return databaseClient.sql("""
                        SELECT count(*) AS cnt FROM game_log
                        WHERE room_id = :roomId
                          AND game_id = (SELECT max(game_id) FROM game_generation WHERE room_id = :roomId)
                        """)
                .bind("roomId", roomId)
                .map(row -> row.get("cnt", Long.class))
                .one().block(TIMEOUT);
    }

    private long snapshotCount(long roomId) {
        return databaseClient.sql("SELECT count(*) AS cnt FROM game_snapshot WHERE room_id = :roomId")
                .bind("roomId", roomId)
                .map(row -> row.get("cnt", Long.class))
                .one().block(TIMEOUT);
    }

    private boolean latestGenerationCompleted(long roomId) {
        return Boolean.TRUE.equals(databaseClient.sql("""
                        SELECT completed FROM game_generation
                        WHERE room_id = :roomId ORDER BY game_id DESC LIMIT 1
                        """)
                .bind("roomId", roomId)
                .map(row -> row.get("completed", Boolean.class))
                .one().block(TIMEOUT));
    }

    private Long currentToken(long roomId) {
        return databaseClient.sql("SELECT fencing_token FROM room_lease WHERE room_id = :roomId")
                .bind("roomId", roomId)
                .map(row -> row.get("fencing_token", Long.class))
                .one().block(TIMEOUT);
    }

    private String leaseOwner(long roomId) {
        return databaseClient.sql("SELECT owner_instance FROM room_lease WHERE room_id = :roomId")
                .bind("roomId", roomId)
                .map(row -> row.get("owner_instance", String.class))
                .one().block(TIMEOUT);
    }
}
