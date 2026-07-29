package com.pomingmatgo.gameservice.gamelog;

import com.pomingmatgo.gameservice.domain.ChoiceInfo;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import com.pomingmatgo.gameservice.domain.repository.PostgresGameGenerations;
import com.pomingmatgo.gameservice.domain.repository.PostgresGameLogRepository;
import com.pomingmatgo.gameservice.domain.repository.PostgresGameSnapshotRepository;
import com.pomingmatgo.gameservice.domain.score.ScoreBreakdown;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 실물 Postgres 대상 통합 테스트 — 미기동이면 전체 skip.
 * 기동 예: docker run -d --name gostop-pg-test -e POSTGRES_PASSWORD=postgres -p 15432:5432 postgres:16-alpine
 * (접속은 GAME_LOG_PG_URL 환경변수로 재정의 가능)
 */
class PostgresGameLogStoreTest {

    private static final String URL = System.getenv().getOrDefault(
            "GAME_LOG_PG_URL", "r2dbc:postgresql://postgres:postgres@localhost:15432/postgres");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private static ConnectionPool pool;
    private static DatabaseClient db;
    private static PostgresGameGenerations generations;
    private static PostgresGameLogRepository logRepository;
    private static PostgresGameSnapshotRepository snapshotRepository;

    @BeforeAll
    static void setUp() {
        assumeTrue(reachable(), "Postgres 미기동 — skip: " + URL);
        pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(URL))
                .initialSize(1).maxSize(4).build());
        db = DatabaseClient.create(pool);
        new ResourceDatabasePopulator(new ClassPathResource("db/game-log-schema.sql")).populate(pool).block(TIMEOUT);
        generations = new PostgresGameGenerations(db);
        logRepository = new PostgresGameLogRepository(db, generations);
        snapshotRepository = new PostgresGameSnapshotRepository(db, generations);
    }

    @AfterAll
    static void tearDown() {
        if (pool != null) {
            pool.dispose();
        }
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

    // DB가 실행 간 보존되므로 방마다 새 id로 격리한다
    private static long newRoomId() {
        return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    }

    @Test
    @DisplayName("배치 append 후 조회하면 레코드가 원형 그대로 돌아온다 (덱·null 필드 포함)")
    void roundTrip() {
        long roomId = newRoomId();
        List<Card> deck = Arrays.asList(Card.values());
        GameLogRecord deckInit = GameLogRecord.deckInit(roomId, 1, deck);
        GameLogRecord submit = GameLogRecord.command(roomId, 2, GameCommandType.NORMAL_SUBMIT,
                Player.PLAYER_1, 3, false, GamePhase.IN_PROGRESS, GamePhase.IN_PROGRESS);
        GameLogRecord goStop = GameLogRecord.command(roomId, 3, GameCommandType.GO_STOP,
                Player.PLAYER_2, 0, true, GamePhase.AWAITING_GO_STOP_CHOICE, GamePhase.END);

        logRepository.append(roomId, List.of(deckInit, submit)).block(TIMEOUT);
        logRepository.append(roomId, List.of(goStop)).block(TIMEOUT);

        List<GameLogRecord> replayed = logRepository.findAllFromSeq(roomId, 1).collectList().block(TIMEOUT);
        assertEquals(List.of(deckInit, submit, goStop), replayed);

        List<GameLogRecord> tail = logRepository.findAllFromSeq(roomId, 3).collectList().block(TIMEOUT);
        assertEquals(List.of(goStop), tail);
    }

    @Test
    @DisplayName("같은 방의 새 게임(DECK_INIT)은 새 세대로 분리되고 이전 세대 레코드는 보존된다")
    void generationSeparation() {
        long roomId = newRoomId();
        logRepository.append(roomId, List.of(
                GameLogRecord.deckInit(roomId, 1, List.of(Card.JAN_1, Card.FEB_2)),
                GameLogRecord.command(roomId, 2, GameCommandType.NORMAL_SUBMIT, Player.PLAYER_1, 0, false,
                        GamePhase.IN_PROGRESS, GamePhase.END))).block(TIMEOUT);
        logRepository.markCompleted(roomId).block(TIMEOUT);

        GameLogRecord gen2Init = GameLogRecord.deckInit(roomId, 1, List.of(Card.MAR_1, Card.APR_2));
        GameLogRecord gen2Submit = GameLogRecord.command(roomId, 2, GameCommandType.NORMAL_SUBMIT,
                Player.PLAYER_2, 1, false, GamePhase.IN_PROGRESS, GamePhase.IN_PROGRESS);
        logRepository.append(roomId, List.of(gen2Init, gen2Submit)).block(TIMEOUT);

        // 조회는 최신 세대만
        assertEquals(List.of(gen2Init, gen2Submit),
                logRepository.findAllFromSeq(roomId, 1).collectList().block(TIMEOUT));

        // cleanup ≠ delete — 이전 세대 레코드는 남는다
        Long totalRows = db.sql("SELECT count(*) AS cnt FROM game_log WHERE room_id = :roomId")
                .bind("roomId", roomId)
                .map(row -> row.get("cnt", Long.class)).one().block(TIMEOUT);
        assertEquals(4L, totalRows);

        // 완료 표시는 세대 단위 — 1세대만 completed
        List<Boolean> completedFlags = db.sql(
                        "SELECT completed FROM game_generation WHERE room_id = :roomId ORDER BY game_id")
                .bind("roomId", roomId)
                .map(row -> row.get("completed", Boolean.class)).all().collectList().block(TIMEOUT);
        assertEquals(List.of(true, false), completedFlags);
    }

    @Test
    @DisplayName("cross-room 배치: 여러 방·세대 경계가 섞인 한 배치가 방·세대별로 올바르게 귀속된다")
    void crossRoomAppendAll() {
        long roomA = newRoomId();
        long roomB = newRoomId();
        logRepository.appendAll(List.of(
                GameLogRecord.deckInit(roomA, 1, List.of(Card.JAN_1)),
                GameLogRecord.deckInit(roomB, 1, List.of(Card.FEB_1)),
                command(roomA, 2),
                command(roomB, 2))).block(TIMEOUT);

        // 같은 배치 안에 roomA의 1세대 마지막 커맨드 + 새 게임(DECK_INIT) + 2세대 커맨드가 섞인 경우
        GameLogRecord gen2Init = GameLogRecord.deckInit(roomA, 1, List.of(Card.MAR_1));
        GameLogRecord gen2Command = command(roomA, 2);
        logRepository.appendAll(List.of(
                command(roomA, 3),
                gen2Init,
                gen2Command,
                command(roomB, 3))).block(TIMEOUT);

        // 조회는 방별 최신 세대만 — roomA는 2세대 2건, roomB는 1세대 3건
        assertEquals(List.of(gen2Init, gen2Command),
                logRepository.findAllFromSeq(roomA, 1).collectList().block(TIMEOUT));
        assertEquals(3, logRepository.findAllFromSeq(roomB, 1).collectList().block(TIMEOUT).size());

        // 이전 세대 레코드 보존 + 세대 수 확인
        Long roomARows = db.sql("SELECT count(*) AS cnt FROM game_log WHERE room_id = :roomId")
                .bind("roomId", roomA)
                .map(row -> row.get("cnt", Long.class)).one().block(TIMEOUT);
        assertEquals(5L, roomARows);
        Long roomAGenerations = db.sql("SELECT count(*) AS cnt FROM game_generation WHERE room_id = :roomId")
                .bind("roomId", roomA)
                .map(row -> row.get("cnt", Long.class)).one().block(TIMEOUT);
        assertEquals(2L, roomAGenerations);
    }

    private GameLogRecord command(long roomId, long seq) {
        return GameLogRecord.command(roomId, seq, GameCommandType.NORMAL_SUBMIT,
                Player.PLAYER_1, 0, false, GamePhase.IN_PROGRESS, GamePhase.IN_PROGRESS);
    }

    @Test
    @DisplayName("스냅샷은 JSON 왕복 후에도 상태가 보존되고 findLatest는 최신 세대의 최고 seq를 반환한다")
    void snapshotRoundTripAndLatest() {
        long roomId = newRoomId();
        logRepository.append(roomId, List.of(GameLogRecord.deckInit(roomId, 1, List.of(Card.JAN_1)))).block(TIMEOUT);

        snapshotRepository.save(snapshotAt(roomId, 2, 3)).block(TIMEOUT);
        GameSnapshot expected = snapshotAt(roomId, 5, 7);
        snapshotRepository.save(expected).block(TIMEOUT);

        GameSnapshot restored = snapshotRepository.findLatest(roomId).block(TIMEOUT);
        assertNotNull(restored);
        assertEquals(expected.roomId(), restored.roomId());
        assertEquals(expected.seq(), restored.seq());
        assertEquals(expected.p1Hand(), restored.p1Hand());
        assertEquals(expected.p2Hand(), restored.p2Hand());
        assertEquals(expected.floorCards(), restored.floorCards());
        assertEquals(expected.hiddenDeck(), restored.hiddenDeck());
        assertEquals(expected.p1Acquired(), restored.p1Acquired());
        assertEquals(expected.p2Acquired(), restored.p2Acquired());

        GameState state = restored.gameState();
        assertEquals(GamePhase.AWAITING_FLOOR_CARD_CHOICE, state.getPhase());
        assertEquals(7, state.getRound());
        assertEquals(1, state.getCurrentTurn());
        assertEquals(2, state.getLeadingPlayer());
        assertEquals(101L, state.getPlayer1().getUserId());
        assertEquals(5, state.getPlayer1().getScore());
        assertEquals(3, state.getPlayer1().getBreakdown().getPiScore());
        assertEquals(102L, state.getPlayer2().getUserId());
        assertEquals(Player.PLAYER_1, state.getChoiceInfo().getPlayerNumToChoose());
        assertEquals(List.of(Card.MAY_3, Card.MAY_4), state.getChoiceInfo().getSelectableCards());
    }

    @Test
    @DisplayName("cross-room 스냅샷 배치: 여러 방이 한 saveAll로 저장되고 세대 없는 방 것만 버려진다")
    void crossRoomSnapshotSaveAll() {
        long roomA = newRoomId();
        long roomB = newRoomId();
        long roomWithoutGeneration = newRoomId();
        logRepository.append(roomA, List.of(GameLogRecord.deckInit(roomA, 1, List.of(Card.JAN_1)))).block(TIMEOUT);
        logRepository.append(roomB, List.of(GameLogRecord.deckInit(roomB, 1, List.of(Card.FEB_1)))).block(TIMEOUT);

        GameSnapshot latestA = snapshotAt(roomA, 5, 3);
        snapshotRepository.saveAll(List.of(
                snapshotAt(roomA, 2, 2),
                latestA,
                snapshotAt(roomB, 2, 2),
                snapshotAt(roomWithoutGeneration, 1, 1))).block(TIMEOUT);

        GameSnapshot restoredA = snapshotRepository.findLatest(roomA).block(TIMEOUT);
        assertNotNull(restoredA);
        assertEquals(latestA.seq(), restoredA.seq());
        assertEquals(latestA.p1Hand(), restoredA.p1Hand());
        GameSnapshot restoredB = snapshotRepository.findLatest(roomB).block(TIMEOUT);
        assertNotNull(restoredB);
        assertEquals(2, restoredB.seq());
        assertNull(snapshotRepository.findLatest(roomWithoutGeneration).block(TIMEOUT));
    }

    @Test
    @DisplayName("세대가 없는 방의 스냅샷 저장은 조용히 버려진다 (유실 = replay 연장일 뿐)")
    void snapshotWithoutGenerationIsDropped() {
        long roomId = newRoomId();
        snapshotRepository.save(snapshotAt(roomId, 1, 1)).block(TIMEOUT);
        assertNull(snapshotRepository.findLatest(roomId).block(TIMEOUT));
    }

    @Test
    @DisplayName("캐시 miss(재시작 상황)여도 DB 폴백으로 현재 세대를 찾아 append/조회가 이어진다")
    void cacheMissFallsBackToDb() {
        long roomId = newRoomId();
        logRepository.append(roomId, List.of(GameLogRecord.deckInit(roomId, 1, List.of(Card.JAN_1)))).block(TIMEOUT);

        // 재시작한 인스턴스 = 캐시가 빈 새 컴포넌트
        PostgresGameGenerations freshGenerations = new PostgresGameGenerations(db);
        PostgresGameLogRepository freshRepository = new PostgresGameLogRepository(db, freshGenerations);

        GameLogRecord late = GameLogRecord.command(roomId, 2, GameCommandType.FLOOR_SELECT,
                Player.PLAYER_1, 0, false, GamePhase.AWAITING_FLOOR_CARD_CHOICE, GamePhase.IN_PROGRESS);
        freshRepository.append(roomId, List.of(late)).block(TIMEOUT);

        assertEquals(2, freshRepository.findAllFromSeq(roomId, 1).collectList().block(TIMEOUT).size());
    }

    private GameSnapshot snapshotAt(long roomId, long seq, int round) {
        GameState state = GameState.builder()
                .roomId(roomId)
                .player1(PlayerState.builder().userId(101L).ready(true).score(5).go(1).goScore(4).ppeokCount(1)
                        .breakdown(ScoreBreakdown.builder().piScore(3).piCount(6).gwangScore(2).gwangCount(3).build())
                        .build())
                .player2(PlayerState.builder().userId(102L).ready(true).score(2).build())
                .leadingPlayer(2)
                .currentTurn(1)
                .round(round)
                .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                .choiceInfo(ChoiceInfo.builder()
                        .playerNumToChoose(Player.PLAYER_1)
                        .submittedCard(Card.MAY_1)
                        .selectableCards(List.of(Card.MAY_3, Card.MAY_4))
                        .turnedCard(Card.MAY_2)
                        .prevCards(List.of(Card.JAN_3))
                        .prevMoveCards(List.of())
                        .build())
                .build();
        return new GameSnapshot(roomId, seq, state,
                List.of(Card.JAN_1, Card.FEB_1), List.of(Card.MAR_1), List.of(Card.APR_1),
                List.of(Card.JUN_1, Card.JUL_1), List.of(Card.AUG_1), List.of());
    }
}
