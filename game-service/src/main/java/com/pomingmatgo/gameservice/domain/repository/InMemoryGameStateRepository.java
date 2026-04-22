package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Profile("in-memory")
@Repository
public class InMemoryGameStateRepository implements GameStateRepository {

    private final ConcurrentHashMap<Long, AtomicReference<GameState>> store = new ConcurrentHashMap<>();

    @Override
    public Mono<GameState> findById(long roomId) {
        return Mono.fromCallable(() -> {
            AtomicReference<GameState> ref = store.get(roomId);
            return ref != null ? ref.get() : null;
        });
    }

    @Override
    public Mono<Long> create(GameState gameState) {
        return Mono.fromCallable(() -> {
            AtomicReference<GameState> existing = store.putIfAbsent(
                    gameState.getRoomId(), new AtomicReference<>(gameState));
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
            AtomicReference<GameState> ref = store.get(gameState.getRoomId());
            if (ref == null) {
                throw new BusinessException(ErrorCode.SYSTEM_ERROR);
            }
            ref.set(gameState);
            return gameState.getRoomId();
        });
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> store.remove(roomId));
    }
}
