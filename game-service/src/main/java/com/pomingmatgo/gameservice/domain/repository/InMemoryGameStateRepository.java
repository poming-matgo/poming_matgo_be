package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

@Profile("in-memory")
@Repository
public class InMemoryGameStateRepository implements GameStateRepository {

    private final ConcurrentHashMap<Long, GameState> store = new ConcurrentHashMap<>();

    @Override
    public Mono<GameState> findById(long roomId) {
        return Mono.fromCallable(() -> store.get(roomId));
    }

    @Override
    public Mono<Long> create(GameState gameState) {
        return Mono.fromCallable(() -> {
            GameState existing = store.putIfAbsent(gameState.getRoomId(), gameState);
            if (existing != null) {
                throw new BusinessException(ErrorCode.ALREADY_EXISTED_ROOM);
            }
            return gameState.getRoomId();
        });
    }

    @Override
    public Mono<Long> delete(long roomId) {
        return Mono.fromCallable(() -> {
            store.remove(roomId);
            return roomId;
        });
    }

    @Override
    public Mono<Long> save(GameState gameState) {
        return Mono.fromCallable(() -> {
            // RoomLockManager 직렬화 보장 → containsKey + put 사이 레이스 없음
            if (!store.containsKey(gameState.getRoomId())) {
                throw new BusinessException(ErrorCode.SYSTEM_ERROR);
            }
            store.put(gameState.getRoomId(), gameState);
            return gameState.getRoomId();
        });
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> store.remove(roomId));
    }
}
