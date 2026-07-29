package com.pomingmatgo.gameservice.snapshot;

import com.pomingmatgo.gameservice.domain.repository.GameSnapshotRepository;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshotWriter;
import com.pomingmatgo.gameservice.global.config.GameLogBatchProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("스냅샷 writer: snapshot-cross-room 배치와 단건 폴백")
class GameSnapshotWriterTest {

    private static class RecordingSnapshotRepository implements GameSnapshotRepository {
        private final List<List<GameSnapshot>> batches = new CopyOnWriteArrayList<>();
        private final List<GameSnapshot> singles = new CopyOnWriteArrayList<>();

        @Override
        public Mono<Void> save(GameSnapshot snapshot) {
            return Mono.fromRunnable(() -> singles.add(snapshot));
        }

        @Override
        public Mono<Void> saveAll(List<GameSnapshot> batch) {
            return Mono.fromRunnable(() -> batches.add(List.copyOf(batch)));
        }

        @Override
        public Mono<GameSnapshot> findLatest(long roomId) {
            return Mono.empty();
        }
    }

    private GameSnapshot snapshot(long roomId, long seq) {
        return new GameSnapshot(roomId, seq, null, List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
    }

    private GameLogBatchProperties properties(int maxSize, Duration flushInterval, boolean snapshotCrossRoom) {
        return new GameLogBatchProperties(maxSize, flushInterval, false, 1, snapshotCrossRoom);
    }

    private void await(BooleanSupplier condition) {
        for (int i = 0; i < 200 && !condition.getAsBoolean(); i++) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(e);
            }
        }
        assertTrue(condition.getAsBoolean());
    }

    @Test
    @DisplayName("배치 on: 여러 방의 스냅샷이 maxSize 도달 시 한 saveAll로 묶인다")
    void batchesAcrossRoomsOnMaxSize() {
        RecordingSnapshotRepository repository = new RecordingSnapshotRepository();
        GameSnapshotWriter writer = new GameSnapshotWriter(repository, properties(3, Duration.ofSeconds(30), true));

        writer.submit(snapshot(1, 5));
        writer.submit(snapshot(2, 7));
        writer.submit(snapshot(3, 9));

        await(() -> !repository.batches.isEmpty());
        assertEquals(List.of(snapshot(1, 5), snapshot(2, 7), snapshot(3, 9)), repository.batches.get(0));
        assertTrue(repository.singles.isEmpty());
    }

    @Test
    @DisplayName("배치 on: maxSize 미달이어도 flush 주기에 saveAll이 나간다")
    void flushesOnInterval() {
        RecordingSnapshotRepository repository = new RecordingSnapshotRepository();
        GameSnapshotWriter writer = new GameSnapshotWriter(repository, properties(64, Duration.ofMillis(30), true));

        writer.submit(snapshot(1, 5));
        writer.submit(snapshot(2, 7));

        await(() -> !repository.batches.isEmpty());
        assertEquals(List.of(snapshot(1, 5), snapshot(2, 7)), repository.batches.get(0));
    }

    @Test
    @DisplayName("배치 off(기본): 기존 단건 fire-and-forget 경로 그대로")
    void fallsBackToSingleSave() {
        RecordingSnapshotRepository repository = new RecordingSnapshotRepository();
        GameSnapshotWriter writer = new GameSnapshotWriter(repository, properties(64, Duration.ofMillis(20), false));

        writer.submit(snapshot(1, 5));

        await(() -> !repository.singles.isEmpty());
        assertEquals(snapshot(1, 5), repository.singles.get(0));
        assertTrue(repository.batches.isEmpty());
    }

    @Test
    @DisplayName("배치 저장 실패는 해당 배치만 버리고 파이프라인은 계속된다")
    void survivesBatchFailure() {
        RecordingSnapshotRepository repository = new RecordingSnapshotRepository() {
            private boolean failedOnce;

            @Override
            public Mono<Void> saveAll(List<GameSnapshot> batch) {
                if (!failedOnce) {
                    failedOnce = true;
                    return Mono.error(new IllegalStateException("주입 실패"));
                }
                return super.saveAll(batch);
            }
        };
        GameSnapshotWriter writer = new GameSnapshotWriter(repository, properties(1, Duration.ofSeconds(30), true));

        writer.submit(snapshot(1, 1));
        writer.submit(snapshot(1, 2));

        await(() -> !repository.batches.isEmpty());
        assertEquals(List.of(snapshot(1, 2)), repository.batches.get(0));
    }
}
