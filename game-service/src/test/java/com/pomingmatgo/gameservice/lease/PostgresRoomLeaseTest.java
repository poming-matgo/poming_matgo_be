package com.pomingmatgo.gameservice.lease;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.event.LeaseLostEvent;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import com.pomingmatgo.gameservice.domain.lease.RoomLeaseManager;
import com.pomingmatgo.gameservice.domain.repository.PostgresGameGenerations;
import com.pomingmatgo.gameservice.domain.repository.PostgresGameLogRepository;
import com.pomingmatgo.gameservice.domain.repository.PostgresGameSnapshotRepository;
import com.pomingmatgo.gameservice.domain.repository.PostgresRoomLeaseRepository;
import com.pomingmatgo.gameservice.domain.repository.RoomLeaseRepository;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import com.pomingmatgo.gameservice.global.config.RoomLeaseProperties;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;
import org.springframework.r2dbc.core.DatabaseClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * lease + fencing token 통합 테스트 (2-A/2-B) — 실물 Postgres 대상, 미기동이면 전체 skip.
 * 기동 예: docker run -d --name gostop-pg-test -e POSTGRES_PASSWORD=postgres -p 15432:5432 postgres:16-alpine
 */
class PostgresRoomLeaseTest {

    private static final String URL = System.getenv().getOrDefault(
            "GAME_LOG_PG_URL", "r2dbc:postgresql://postgres:postgres@localhost:15432/postgres");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final RoomLeaseProperties PROPS =
            new RoomLeaseProperties(Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofMillis(20));

    private static ConnectionPool pool;
    private static DatabaseClient db;
    private static PostgresRoomLeaseRepository leaseRepository;

    private final List<Object> publishedEvents = new ArrayList<>();

    @BeforeAll
    static void setUp() {
        assumeTrue(reachable(), "Postgres 미기동 — skip: " + URL);
        pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(URL))
                .initialSize(1).maxSize(4).build());
        db = DatabaseClient.create(pool);
        new ResourceDatabasePopulator(new ClassPathResource("db/game-log-schema.sql")).populate(pool).block(TIMEOUT);
        leaseRepository = new PostgresRoomLeaseRepository(db);
    }

    @AfterAll
    static void tearDown() {
        if (pool != null) {
            pool.dispose();
        }
    }

    @BeforeEach
    void resetEvents() {
        publishedEvents.clear();
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

    private RoomLeaseManager newManager() {
        return new RoomLeaseManager(leaseRepository, PROPS, publishedEvents::add);
    }

    // 만료를 기다리지 않고 강제 — 인수(takeover) 상황 재현
    private void forceExpire(long roomId) {
        db.sql("UPDATE room_lease SET expires_at = now() - interval '1 second' WHERE room_id = :roomId")
                .bind("roomId", roomId).then().block(TIMEOUT);
    }

    // ── 2-A: lease 획득/연장/해제 의미론 ─────────────────────────────────────────

    @Test
    @DisplayName("acquire: 최초 1, 같은 소유자 재획득(같은 방 새 게임)·만료 후 인수 모두 token이 단조 증가한다")
    void acquireTokenIsMonotonic() {
        long roomId = newRoomId();
        assertEquals(1L, leaseRepository.acquire(roomId, "A", PROPS.duration()).block(TIMEOUT));
        assertEquals(2L, leaseRepository.acquire(roomId, "A", PROPS.duration()).block(TIMEOUT));

        forceExpire(roomId);
        assertEquals(3L, leaseRepository.acquire(roomId, "B", PROPS.duration()).block(TIMEOUT));
    }

    @Test
    @DisplayName("acquire: 다른 인스턴스의 유효 lease가 있으면 거부(empty)된다")
    void acquireRejectedWhileHeldByOther() {
        long roomId = newRoomId();
        leaseRepository.acquire(roomId, "A", PROPS.duration()).block(TIMEOUT);
        assertNull(leaseRepository.acquire(roomId, "B", PROPS.duration()).block(TIMEOUT));
        // 원 소유자는 여전히 token 2로 재획득 가능
        assertEquals(2L, leaseRepository.acquire(roomId, "A", PROPS.duration()).block(TIMEOUT));
    }

    @Test
    @DisplayName("release: 행을 지우지 않고 즉시 만료 — 재획득 시 token 단조 증가가 보존된다")
    void releaseKeepsRowAndMonotonicity() {
        long roomId = newRoomId();
        long token = leaseRepository.acquire(roomId, "A", PROPS.duration()).block(TIMEOUT);
        leaseRepository.release(roomId, token).block(TIMEOUT);

        // 만료됐으므로 누구든 즉시 인수 가능, token은 이어서 증가
        assertEquals(token + 1, leaseRepository.acquire(roomId, "B", PROPS.duration()).block(TIMEOUT));
    }

    @Test
    @DisplayName("heartbeat: 자기 소유 유효 lease만 연장하고, 해제(released)된 lease는 되살리지 않는다")
    void heartbeatExtendsOnlyOwnValidLeases() {
        // DB가 실행 간 보존되므로 owner도 테스트 고유 이름으로 격리한다
        String me = "hb-" + newRoomId();
        String someoneElse = "hb-other-" + newRoomId();
        long owned = newRoomId();
        long released = newRoomId();
        long other = newRoomId();
        leaseRepository.acquire(owned, me, Duration.ofSeconds(2)).block(TIMEOUT);
        long releasedToken = leaseRepository.acquire(released, me, PROPS.duration()).block(TIMEOUT);
        leaseRepository.release(released, releasedToken).block(TIMEOUT);
        leaseRepository.acquire(other, someoneElse, Duration.ofSeconds(2)).block(TIMEOUT);

        assertEquals(1L, leaseRepository.heartbeat(me, PROPS.duration()).block(TIMEOUT));

        // 연장된 lease만 만료 시점이 늘어났다 (2s → 30s 뒤)
        assertTrue(remainingSeconds(owned) > 10);
        assertTrue(remainingSeconds(released) <= 0);
        assertTrue(remainingSeconds(other) < 10);
    }

    private double remainingSeconds(long roomId) {
        return db.sql("SELECT extract(epoch FROM (expires_at - now())) AS remaining FROM room_lease WHERE room_id = :roomId")
                .bind("roomId", roomId)
                .map(row -> row.get("remaining", Double.class))
                .one().block(TIMEOUT);
    }

    // ── 2-B: fencing token — 좀비 쓰기 차단 ─────────────────────────────────────

    @Test
    @DisplayName("fencing: 소유 중엔 append가 통과하고, 인수당한 뒤엔 거부 + LeaseLostEvent + 토큰 캐시 회수")
    void fencedAppendDetectsOwnershipLoss() {
        long roomId = newRoomId();
        RoomLeaseManager zombie = newManager();
        PostgresGameGenerations generations = new PostgresGameGenerations(db);
        PostgresGameLogRepository repository = new PostgresGameLogRepository(db, generations, zombie);

        zombie.acquire(roomId).block(TIMEOUT);
        repository.appendAll(List.of(
                GameLogRecord.deckInit(roomId, 1, List.of(Card.JAN_1), null, null, 1),
                command(roomId, 2))).block(TIMEOUT);
        assertEquals(2, repository.findAllFromSeq(roomId, 1).collectList().block(TIMEOUT).size());

        // 다른 인스턴스가 인수 (GC pause 등으로 lease가 만료된 상황)
        forceExpire(roomId);
        newManager().acquire(roomId).block(TIMEOUT);

        repository.appendAll(List.of(command(roomId, 3))).block(TIMEOUT);

        // 좀비의 쓰기는 DB가 거부했고, 상실이 통보됐으며, 캐시가 회수돼 이후 쓰기는 시도조차 안 된다
        assertEquals(2, repository.findAllFromSeq(roomId, 1).collectList().block(TIMEOUT).size());
        assertEquals(List.of(new LeaseLostEvent(roomId)), publishedEvents);
        assertNull(zombie.tokenOf(roomId));

        // 상실 후 markCompleted도 no-op — 인수자의 진행 중 세대를 완료로 오염시키지 않는다
        repository.markCompleted(roomId).block(TIMEOUT);
        assertEquals(false, db.sql("SELECT completed FROM game_generation WHERE room_id = :roomId")
                .bind("roomId", roomId).map(row -> row.get("completed", Boolean.class)).one().block(TIMEOUT));
    }

    @Test
    @DisplayName("fencing: 좀비의 DECK_INIT은 세대 발급 자체가 거부돼 인수자의 세대 조회를 오염시키지 못한다")
    void zombieDeckInitCannotPolluteGenerations() {
        long roomId = newRoomId();
        RoomLeaseManager zombie = newManager();
        PostgresGameGenerations generations = new PostgresGameGenerations(db);
        PostgresGameLogRepository repository = new PostgresGameLogRepository(db, generations, zombie);

        zombie.acquire(roomId).block(TIMEOUT);
        repository.appendAll(List.of(GameLogRecord.deckInit(roomId, 1, List.of(Card.JAN_1), null, null, 1))).block(TIMEOUT);

        forceExpire(roomId);
        newManager().acquire(roomId).block(TIMEOUT);

        repository.appendAll(List.of(GameLogRecord.deckInit(roomId, 1, List.of(Card.FEB_1), null, null, 1))).block(TIMEOUT);

        Long generationCount = db.sql("SELECT count(*) AS cnt FROM game_generation WHERE room_id = :roomId")
                .bind("roomId", roomId).map(row -> row.get("cnt", Long.class)).one().block(TIMEOUT);
        assertEquals(1L, generationCount);
        assertEquals(List.of(new LeaseLostEvent(roomId)), publishedEvents);
    }

    @Test
    @DisplayName("fencing: cross-room 배치에서 인수당한 방의 행만 빠지고 소유 중인 방의 행은 들어간다")
    void crossRoomBatchDropsOnlyFencedRoom() {
        long ownedRoom = newRoomId();
        long hijackedRoom = newRoomId();
        RoomLeaseManager manager = newManager();
        PostgresGameGenerations generations = new PostgresGameGenerations(db);
        PostgresGameLogRepository repository = new PostgresGameLogRepository(db, generations, manager);

        manager.acquire(ownedRoom).block(TIMEOUT);
        manager.acquire(hijackedRoom).block(TIMEOUT);
        repository.appendAll(List.of(
                GameLogRecord.deckInit(ownedRoom, 1, List.of(Card.JAN_1), null, null, 1),
                GameLogRecord.deckInit(hijackedRoom, 1, List.of(Card.FEB_1), null, null, 1))).block(TIMEOUT);

        forceExpire(hijackedRoom);
        newManager().acquire(hijackedRoom).block(TIMEOUT);

        repository.appendAll(List.of(
                command(ownedRoom, 2),
                command(hijackedRoom, 2))).block(TIMEOUT);

        assertEquals(2, repository.findAllFromSeq(ownedRoom, 1).collectList().block(TIMEOUT).size());
        assertEquals(1, repository.findAllFromSeq(hijackedRoom, 1).collectList().block(TIMEOUT).size());
        assertEquals(List.of(new LeaseLostEvent(hijackedRoom)), publishedEvents);
        assertNotNull(manager.tokenOf(ownedRoom));
    }

    @Test
    @DisplayName("fencing: 좀비의 스냅샷은 조용히 버려진다 (유실 허용 계약 — 상실 통보는 로그 경로 몫)")
    void zombieSnapshotIsSilentlyDropped() {
        long roomId = newRoomId();
        RoomLeaseManager zombie = newManager();
        PostgresGameGenerations generations = new PostgresGameGenerations(db);
        PostgresGameLogRepository logRepository = new PostgresGameLogRepository(db, generations, zombie);
        PostgresGameSnapshotRepository snapshotRepository = new PostgresGameSnapshotRepository(db, generations, zombie);

        zombie.acquire(roomId).block(TIMEOUT);
        logRepository.appendAll(List.of(GameLogRecord.deckInit(roomId, 1, List.of(Card.JAN_1), null, null, 1))).block(TIMEOUT);
        snapshotRepository.save(snapshotAt(roomId, 1)).block(TIMEOUT);
        assertNotNull(snapshotRepository.findLatest(roomId).block(TIMEOUT));

        forceExpire(roomId);
        newManager().acquire(roomId).block(TIMEOUT);

        snapshotRepository.save(snapshotAt(roomId, 5)).block(TIMEOUT);
        assertEquals(1, snapshotRepository.findLatest(roomId).block(TIMEOUT).seq());
        assertTrue(publishedEvents.isEmpty());
    }

    @Test
    @DisplayName("release는 조립 시점이 아니라 구독 시점에 토큰을 회수한다 — cleanup 체인(.then 인자)의 마지막 쓰기 보호")
    void releaseEvictsTokenOnlyOnSubscription() {
        long roomId = newRoomId();
        RoomLeaseManager manager = newManager();
        manager.acquire(roomId).block(TIMEOUT);

        var release = manager.release(roomId);
        // 조립만으로 회수되면 아직 drain 중인 마지막 로그 배치와 markCompleted가 자기 토큰 없이 막힌다 (실제 있었던 버그)
        assertNotNull(manager.tokenOf(roomId));
        release.block(TIMEOUT);
        assertNull(manager.tokenOf(roomId));
    }

    @Test
    @DisplayName("정상 종료 흐름: 소유 중 markCompleted는 통과하고 release 후 lease는 만료 상태로 남는다")
    void ownerCompletesAndReleases() {
        long roomId = newRoomId();
        RoomLeaseManager manager = newManager();
        PostgresGameGenerations generations = new PostgresGameGenerations(db);
        PostgresGameLogRepository repository = new PostgresGameLogRepository(db, generations, manager);

        manager.acquire(roomId).block(TIMEOUT);
        repository.appendAll(List.of(GameLogRecord.deckInit(roomId, 1, List.of(Card.JAN_1), null, null, 1))).block(TIMEOUT);
        repository.markCompleted(roomId).block(TIMEOUT);
        manager.release(roomId).block(TIMEOUT);

        assertEquals(true, db.sql("SELECT completed FROM game_generation WHERE room_id = :roomId")
                .bind("roomId", roomId).map(row -> row.get("completed", Boolean.class)).one().block(TIMEOUT));
        assertTrue(remainingSeconds(roomId) <= 0);
        assertTrue(publishedEvents.isEmpty());
    }

    // ── 2-C/2-D: 턴 deadline 기록 · 만료 스캔 · 인수(takeover) ─────────────────

    @Test
    @DisplayName("takeover: 만료 lease만 인수된다 — 유효 lease·released·인수 직후 재인수 모두 거부, token은 이어서 증가")
    void takeoverOnlyClaimsExpiredLeases() {
        long roomId = newRoomId();
        leaseRepository.acquire(roomId, "A", PROPS.duration()).block(TIMEOUT);
        assertNull(leaseRepository.takeover(roomId, "B", PROPS.duration()).block(TIMEOUT));

        forceExpire(roomId);
        RoomLeaseRepository.Takeover takeover = leaseRepository.takeover(roomId, "B", PROPS.duration()).block(TIMEOUT);
        assertEquals(2L, takeover.fencingToken());
        assertNull(takeover.turnDeadlineEpochMillis());
        // 인수 성공 = 새 유효 lease — 경쟁자의 뒤늦은 인수는 거부된다 (UPDATE의 만료 조건이 상호 배제)
        assertNull(leaseRepository.takeover(roomId, "C", PROPS.duration()).block(TIMEOUT));

        // 정상 해제된 lease는 인수 대상이 아니다 — 해제는 cleanup 완주의 증거
        long released = newRoomId();
        long releasedToken = leaseRepository.acquire(released, "A", PROPS.duration()).block(TIMEOUT);
        leaseRepository.release(released, releasedToken).block(TIMEOUT);
        assertNull(leaseRepository.takeover(released, "B", PROPS.duration()).block(TIMEOUT));
    }

    @Test
    @DisplayName("findExpiredRoomIds: 만료+미해제 방만 잡힌다 — 유효 lease와 released는 제외")
    void expiredScanSkipsValidAndReleased() {
        long expired = newRoomId();
        long valid = newRoomId();
        long released = newRoomId();
        leaseRepository.acquire(expired, "A", PROPS.duration()).block(TIMEOUT);
        forceExpire(expired);
        leaseRepository.acquire(valid, "A", PROPS.duration()).block(TIMEOUT);
        long releasedToken = leaseRepository.acquire(released, "A", PROPS.duration()).block(TIMEOUT);
        leaseRepository.release(released, releasedToken).block(TIMEOUT);

        List<Long> found = leaseRepository.findExpiredRoomIds().collectList().block(TIMEOUT);
        assertTrue(found.contains(expired));
        assertFalse(found.contains(valid));
        assertFalse(found.contains(released));
    }

    @Test
    @DisplayName("recordDeadlines: 소유 token 방만 갱신되고(fencing) 인수가 그 값을 돌려준다 — 2-C의 왕복")
    void deadlineRoundTripsThroughTakeover() {
        long owned = newRoomId();
        long hijacked = newRoomId();
        long ownedToken = leaseRepository.acquire(owned, "A", PROPS.duration()).block(TIMEOUT);
        long staleToken = leaseRepository.acquire(hijacked, "A", PROPS.duration()).block(TIMEOUT);
        forceExpire(hijacked);
        leaseRepository.acquire(hijacked, "B", PROPS.duration()).block(TIMEOUT);

        long deadline = 1_700_000_000_000L;
        leaseRepository.recordDeadlines(List.of(
                new RoomLeaseRepository.RoomDeadline(owned, ownedToken, deadline),
                new RoomLeaseRepository.RoomDeadline(hijacked, staleToken, 9L))).block(TIMEOUT);

        assertEquals(deadline, recordedDeadline(owned));
        assertNull(recordedDeadline(hijacked));

        forceExpire(owned);
        RoomLeaseRepository.Takeover takeover = leaseRepository.takeover(owned, "B", PROPS.duration()).block(TIMEOUT);
        assertEquals(deadline, takeover.turnDeadlineEpochMillis());
    }

    @Test
    @DisplayName("abandon: owner를 남긴 채 즉시 만료 — 스캔에 다시 잡혀 재인수(복구 재시도)가 가능하다")
    void abandonKeepsRoomRecoverable() {
        long roomId = newRoomId();
        long token = leaseRepository.acquire(roomId, "A", PROPS.duration()).block(TIMEOUT);
        leaseRepository.abandon(roomId, token).block(TIMEOUT);

        assertTrue(leaseRepository.findExpiredRoomIds().collectList().block(TIMEOUT).contains(roomId));
        assertEquals(token + 1, leaseRepository.takeover(roomId, "B", PROPS.duration()).block(TIMEOUT).fencingToken());
    }

    private Long recordedDeadline(long roomId) {
        // r2dbc map은 null 반환을 허용하지 않으므로 Optional로 감싼다
        return db.sql("SELECT turn_deadline_epoch_millis FROM room_lease WHERE room_id = :roomId")
                .bind("roomId", roomId)
                .map(row -> java.util.Optional.ofNullable(row.get("turn_deadline_epoch_millis", Long.class)))
                .one().block(TIMEOUT).orElse(null);
    }

    private GameLogRecord command(long roomId, long seq) {
        return GameLogRecord.command(roomId, seq, GameCommandType.NORMAL_SUBMIT,
                Player.PLAYER_1, 0, false, GamePhase.IN_PROGRESS, GamePhase.IN_PROGRESS);
    }

    private GameSnapshot snapshotAt(long roomId, long seq) {
        GameState state = GameState.builder()
                .roomId(roomId)
                .currentTurn(1)
                .round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        return new GameSnapshot(roomId, seq, state,
                List.of(Card.JAN_1), List.of(Card.FEB_1), List.of(Card.MAR_1),
                List.of(Card.APR_1), List.of(), List.of());
    }
}
