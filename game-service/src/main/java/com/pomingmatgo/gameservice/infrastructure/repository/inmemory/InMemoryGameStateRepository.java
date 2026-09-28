package com.pomingmatgo.gameservice.infrastructure.repository.inmemory;

import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import lombok.RequiredArgsConstructor;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

@Profile("in-memory")
@Repository
@RequiredArgsConstructor
public class InMemoryGameStateRepository implements GameStateRepository {

    private final RoomTimerLifecycle timerLifecycle;
    private final InMemoryRoomExecutionGate executionGate;

    private final ConcurrentHashMap<Long, GameState> store = new ConcurrentHashMap<>();

    @Override
    public Mono<GameState> findById(long roomId) {
        return Mono.fromCallable(() -> store.get(roomId));
    }

    @Override
    public Mono<Long> create(GameState gameState) {
        return Mono.deferContextual(context -> Mono.fromCallable(() -> executionGate.create(gameState.getRoomId(), context, () -> {
            synchronized (timerLifecycle) {
                GameState existing = store.putIfAbsent(gameState.getRoomId(), gameState);
                if (existing != null) {
                    throw new BusinessException(ErrorCode.ALREADY_EXISTED_ROOM);
                }
                timerLifecycle.open(gameState.getRoomId());
            }
            return gameState.getRoomId();
        })));
    }

    @Override
    public Mono<Long> save(GameState gameState) {
        return Mono.fromCallable(() -> {
            // 실행 gate 밖의 직접 호출에서도 삭제된 방을 부활시키지 않는다 (Redis setIfPresent와 같은 계약).
            GameState updated = store.computeIfPresent(gameState.getRoomId(), (k, prev) -> gameState);
            if (updated == null) {
                throw new BusinessException(ErrorCode.SYSTEM_ERROR);
            }
            return gameState.getRoomId();
        });
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> {
            synchronized (timerLifecycle) {
                timerLifecycle.close(roomId);
                store.remove(roomId);
            }
        });
    }
}
