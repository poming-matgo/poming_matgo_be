package com.pomingmatgo.gameservice.gamelog;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandLog;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import com.pomingmatgo.gameservice.domain.repository.GameLogRepository;
import com.pomingmatgo.gameservice.domain.repository.InMemoryGameLogRepository;
import com.pomingmatgo.gameservice.global.config.GameLogBatchProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("커맨드 로그 ordered writer: 방 단위 append 순서·배치·drain·no-op 무비용 + cross-room 배치")
class GameCommandLogWriterTest {

    private static final long ROOM_ID = 960_001L;
    private static final int RECORD_COUNT = 200;
    private static final int BATCH_MAX_SIZE = 64;
    private static final GameLogBatchProperties BATCH_PROPERTIES =
            new GameLogBatchProperties(BATCH_MAX_SIZE, Duration.ofMillis(20), false, 8, false);

    private static class RecordingRepository implements GameLogRepository {
        final List<List<GameLogRecord>> batches = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger recordsWhenCompleted = new AtomicInteger(-1);
        final Duration appendDelay;

        RecordingRepository(Duration appendDelay) {
            this.appendDelay = appendDelay;
        }

        @Override
        public Mono<Void> append(long roomId, List<GameLogRecord> batch) {
            return Mono.delay(appendDelay)
                    .then(Mono.fromRunnable(() -> batches.add(List.copyOf(batch))));
        }

        @Override
        public Flux<GameLogRecord> findAllFromSeq(long roomId, long fromSeq) {
            return Flux.empty();
        }

        @Override
        public Mono<Void> markCompleted(long roomId) {
            return Mono.fromRunnable(() -> recordsWhenCompleted.set(totalRecords()));
        }

        int totalRecords() {
            synchronized (batches) {
                return batches.stream().mapToInt(List::size).sum();
            }
        }
    }

    @Test
    @DisplayName("빠른 emit + 느린 append에도 전 레코드가 seq 순서대로, 배치 상한을 지켜 저장된다")
    void appendsAllRecordsInSeqOrder() {
        RecordingRepository repository = new RecordingRepository(Duration.ofMillis(5));
        GameCommandLog commandLog = new GameCommandLog(repository, BATCH_PROPERTIES);

        for (int i = 0; i < RECORD_COUNT; i++) {
            commandLog.logCommand(ROOM_ID, GameCommandType.NORMAL_SUBMIT, Player.PLAYER_1, i % 10,
                    false, GamePhase.IN_PROGRESS, GamePhase.IN_PROGRESS).block();
        }
        commandLog.close(ROOM_ID).block(Duration.ofSeconds(10));

        List<GameLogRecord> flattened;
        synchronized (repository.batches) {
            flattened = repository.batches.stream().flatMap(List::stream).toList();
            repository.batches.forEach(batch ->
                    assertTrue(batch.size() <= BATCH_MAX_SIZE, "배치 상한 초과: " + batch.size()));
        }

        assertEquals(RECORD_COUNT, flattened.size());
        for (int i = 0; i < flattened.size(); i++) {
            assertEquals(i + 1, flattened.get(i).seq(), "seq 순서 역전 또는 결번");
        }
        assertEquals(RECORD_COUNT, repository.recordsWhenCompleted.get(),
                "완료 표시는 잔여 배치가 전부 append된 뒤여야 한다");
    }

    @Test
    @DisplayName("no-op 저장소(enabled=false)면 채널·append 없이 즉시 완료된다 — 부하 기준선 직교성")
    void noOpStoreBypassesEverything() {
        GameLogRepository noOp = new GameLogRepository() {
            @Override
            public Mono<Void> append(long roomId, List<GameLogRecord> batch) {
                return Mono.error(new AssertionError("no-op 경로에서 append가 호출되면 안 된다"));
            }

            @Override
            public Flux<GameLogRecord> findAllFromSeq(long roomId, long fromSeq) {
                return Flux.empty();
            }

            @Override
            public Mono<Void> markCompleted(long roomId) {
                return Mono.error(new AssertionError("no-op 경로에서 markCompleted가 호출되면 안 된다"));
            }

            @Override
            public boolean enabled() {
                return false;
            }
        };
        // cross-room 모드여도 enabled=false면 채널 자체가 만들어지지 않아야 한다
        GameCommandLog commandLog = new GameCommandLog(noOp,
                new GameLogBatchProperties(BATCH_MAX_SIZE, Duration.ofMillis(20), true, 8, false));

        commandLog.logDeckInit(ROOM_ID, List.of()).block();
        commandLog.logCommand(ROOM_ID, GameCommandType.GO_STOP, Player.PLAYER_1, 0,
                true, GamePhase.AWAITING_GO_STOP_CHOICE, GamePhase.IN_PROGRESS).block();
        assertDoesNotThrow(() -> commandLog.close(ROOM_ID).block(Duration.ofSeconds(1)));
    }

    private static class CrossRoomRecordingRepository implements GameLogRepository {
        final List<List<GameLogRecord>> batches = Collections.synchronizedList(new ArrayList<>());
        final Map<Long, Integer> recordsWhenCompleted = new ConcurrentHashMap<>();

        @Override
        public Mono<Void> appendAll(List<GameLogRecord> batch) {
            return Mono.delay(Duration.ofMillis(5))
                    .then(Mono.fromRunnable(() -> batches.add(List.copyOf(batch))));
        }

        @Override
        public Mono<Void> append(long roomId, List<GameLogRecord> batch) {
            return Mono.error(new AssertionError("cross-room 모드에선 appendAll만 호출돼야 한다"));
        }

        @Override
        public Flux<GameLogRecord> findAllFromSeq(long roomId, long fromSeq) {
            return Flux.empty();
        }

        @Override
        public Mono<Void> markCompleted(long roomId) {
            return Mono.fromRunnable(() -> recordsWhenCompleted.put(roomId, storedRecords(roomId)));
        }

        int storedRecords(long roomId) {
            synchronized (batches) {
                return (int) batches.stream().flatMap(List::stream)
                        .filter(record -> record.roomId() == roomId).count();
            }
        }
    }

    @Test
    @DisplayName("cross-room: 여러 방이 한 배치로 묶여도 방별 seq 순서가 보존되고, close는 그 방의 잔여 flush 후 완료 표시한다")
    void crossRoomBatchesPreservePerRoomOrder() {
        long roomA = 960_101L;
        long roomB = 960_102L;
        CrossRoomRecordingRepository repository = new CrossRoomRecordingRepository();
        GameCommandLog commandLog = new GameCommandLog(repository,
                new GameLogBatchProperties(BATCH_MAX_SIZE, Duration.ofMillis(20), true, 1, false));

        for (int i = 0; i < RECORD_COUNT; i++) {
            long roomId = i % 2 == 0 ? roomA : roomB;
            commandLog.logCommand(roomId, GameCommandType.NORMAL_SUBMIT, Player.PLAYER_1, i % 10,
                    false, GamePhase.IN_PROGRESS, GamePhase.IN_PROGRESS).block();
        }
        commandLog.close(roomA).block(Duration.ofSeconds(10));
        commandLog.close(roomB).block(Duration.ofSeconds(10));

        List<GameLogRecord> flattened;
        synchronized (repository.batches) {
            flattened = repository.batches.stream().flatMap(List::stream).toList();
            repository.batches.forEach(batch ->
                    assertTrue(batch.size() <= BATCH_MAX_SIZE, "배치 상한 초과: " + batch.size()));
        }
        assertEquals(RECORD_COUNT, flattened.size());
        for (long roomId : new long[]{roomA, roomB}) {
            List<GameLogRecord> roomRecords = flattened.stream().filter(r -> r.roomId() == roomId).toList();
            assertEquals(RECORD_COUNT / 2, roomRecords.size());
            for (int i = 0; i < roomRecords.size(); i++) {
                assertEquals(i + 1, roomRecords.get(i).seq(), "방별 seq 순서 역전 또는 결번");
            }
        }
        assertTrue(repository.batches.stream().anyMatch(batch ->
                        batch.stream().map(GameLogRecord::roomId).distinct().count() > 1),
                "두 방이 한 배치로 묶인 경우가 있어야 한다");
        assertEquals(RECORD_COUNT / 2, repository.recordsWhenCompleted.get(roomA),
                "완료 표시는 그 방의 잔여 배치가 전부 append된 뒤여야 한다");
        assertEquals(RECORD_COUNT / 2, repository.recordsWhenCompleted.get(roomB));
    }

    @Test
    @DisplayName("기본 appendAll은 DECK_INIT 경계로 세그먼트를 끊는다 — 한 배치에 섞인 세대 교체가 in-memory 스토어에 올바르게 반영")
    void defaultAppendAllSegmentsAtGenerationBoundary() {
        long roomA = 960_201L;
        long roomB = 960_202L;
        InMemoryGameLogRepository repository = new InMemoryGameLogRepository();

        repository.appendAll(List.of(
                GameLogRecord.deckInit(roomA, 1, List.of(Card.JAN_1)),
                GameLogRecord.command(roomA, 2, GameCommandType.NORMAL_SUBMIT, Player.PLAYER_1, 0,
                        false, GamePhase.IN_PROGRESS, GamePhase.END),
                GameLogRecord.deckInit(roomB, 1, List.of(Card.FEB_1)),
                // 같은 배치 안에서 roomA의 새 게임 시작 — 이전 세대와 같은 세그먼트로 묶이면 안 된다
                GameLogRecord.deckInit(roomA, 1, List.of(Card.MAR_1)),
                GameLogRecord.command(roomA, 2, GameCommandType.NORMAL_SUBMIT, Player.PLAYER_2, 1,
                        false, GamePhase.IN_PROGRESS, GamePhase.IN_PROGRESS)
        )).block(Duration.ofSeconds(5));

        List<GameLogRecord> roomALatest = repository.findAllFromSeq(roomA, 1).collectList().block(Duration.ofSeconds(5));
        assertEquals(2, roomALatest.size(), "roomA는 최신 세대 2건만 보여야 한다");
        assertEquals(List.of(Card.MAR_1), roomALatest.get(0).deck(), "최신 세대의 덱이어야 한다");
        assertEquals(1, repository.findAllFromSeq(roomB, 1).collectList().block(Duration.ofSeconds(5)).size());
    }
}
