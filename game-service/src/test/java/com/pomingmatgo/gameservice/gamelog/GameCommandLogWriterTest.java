package com.pomingmatgo.gameservice.gamelog;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandLog;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import com.pomingmatgo.gameservice.domain.repository.GameLogRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("커맨드 로그 ordered writer: 방 단위 append 순서·배치·drain·no-op 무비용")
class GameCommandLogWriterTest {

    private static final long ROOM_ID = 960_001L;
    private static final int RECORD_COUNT = 200;
    private static final int BATCH_MAX_SIZE = 64;

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
        GameCommandLog commandLog = new GameCommandLog(repository);

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
        GameCommandLog commandLog = new GameCommandLog(noOp);

        commandLog.logDeckInit(ROOM_ID, List.of()).block();
        commandLog.logCommand(ROOM_ID, GameCommandType.GO_STOP, Player.PLAYER_1, 0,
                true, GamePhase.AWAITING_GO_STOP_CHOICE, GamePhase.IN_PROGRESS).block();
        assertDoesNotThrow(() -> commandLog.close(ROOM_ID).block(Duration.ofSeconds(1)));
    }
}
