package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.GameState;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class GameStateRepository {
    private final ReactiveRedisOperations<String, Object> redisOps;

    private static final String GAME_STATE_KEY_PREFIX = "gameState:";
    private static final String GAME_STATE_ID_KEY = "gameState:roomId";

    public Mono<Long> save(GameState gameState) {
        if (gameState.getRoomId() == 0) {
            return generateNewId()
                    .flatMap(newId -> {
                        gameState.setRoomId(newId);
                        return saveGameState(gameState);
                    });
        } else {
            return saveGameState(gameState);
        }
    }

    private Mono<Long> generateNewId() {
        return redisOps.opsForValue()
                .increment(GAME_STATE_ID_KEY)
                .switchIfEmpty(Mono.error(new IllegalStateException()));
    }

    private Mono<Long> saveGameState(GameState gameState) {
        return redisOps.opsForValue()
                .set(GAME_STATE_KEY_PREFIX + gameState.getRoomId(), gameState)
                .flatMap(success -> {
                    if (success) {
                        return Mono.just(gameState.getRoomId());
                    } else {
                        return Mono.error(new IllegalStateException());
                    }
                });
    }
}