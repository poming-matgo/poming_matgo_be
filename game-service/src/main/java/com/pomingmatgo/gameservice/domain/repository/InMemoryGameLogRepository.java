package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

// 개발·테스트용 durability 스탠드인 (실물 저장소는 Postgres) — 방별 현재 게임 세대만 보관한다
@Component
@ConditionalOnProperty(name = "game.log.store", havingValue = "in-memory")
public class InMemoryGameLogRepository implements GameLogRepository {

    private final ConcurrentHashMap<Long, RoomLog> logs = new ConcurrentHashMap<>();

    private static final class RoomLog {
        private final List<GameLogRecord> records = new ArrayList<>();
        private volatile boolean completed;
    }

    @Override
    public Mono<Void> append(long roomId, List<GameLogRecord> batch) {
        return Mono.fromRunnable(() -> {
            // 같은 방의 새 게임은 DECK_INIT으로 시작한다 — 이전 세대는 교체 (dev 스토어라 보존하지 않음)
            RoomLog roomLog = logs.compute(roomId,
                    (k, existing) -> existing == null || startsNewGeneration(batch) ? new RoomLog() : existing);
            synchronized (roomLog.records) {
                roomLog.records.addAll(batch);
            }
        });
    }

    private boolean startsNewGeneration(List<GameLogRecord> batch) {
        return !batch.isEmpty() && batch.get(0).type() == GameCommandType.DECK_INIT;
    }

    @Override
    public Flux<GameLogRecord> findAllFromSeq(long roomId, long fromSeq) {
        return Flux.defer(() -> {
            RoomLog roomLog = logs.get(roomId);
            if (roomLog == null) {
                return Flux.empty();
            }
            List<GameLogRecord> copied;
            synchronized (roomLog.records) {
                copied = new ArrayList<>(roomLog.records);
            }
            return Flux.fromIterable(copied).filter(record -> record.seq() >= fromSeq);
        });
    }

    @Override
    public Mono<Void> markCompleted(long roomId) {
        return Mono.fromRunnable(() -> {
            RoomLog roomLog = logs.get(roomId);
            if (roomLog != null) {
                roomLog.completed = true;
            }
        });
    }

    @Override
    public Mono<Boolean> latestGenerationCompleted(long roomId) {
        return Mono.defer(() -> {
            RoomLog roomLog = logs.get(roomId);
            return roomLog == null ? Mono.empty() : Mono.just(roomLog.completed);
        });
    }

    public boolean isCompleted(long roomId) {
        RoomLog roomLog = logs.get(roomId);
        return roomLog != null && roomLog.completed;
    }
}
