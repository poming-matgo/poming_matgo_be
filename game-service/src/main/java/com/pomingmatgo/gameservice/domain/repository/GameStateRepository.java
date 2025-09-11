package com.pomingmatgo.gameservice.domain.repository;
import com.pomingmatgo.gameservice.domain.GameState;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class GameStateRepository {
    private final ReactiveRedisOperations<String, GameState> redisOps;

    private static final String GAME_STATE_KEY_PREFIX = "gameState:";

    public Mono<GameState> findById(long roomId) {
        String redisKey = GAME_STATE_KEY_PREFIX + roomId;

        return redisOps.opsForValue().get(redisKey);
    }

    public Mono<Long> create(GameState gameState) {
        String redisKey = GAME_STATE_KEY_PREFIX + gameState.getRoomId();
        return checkKeyExists(redisKey)
                .flatMap(exists -> exists ? Mono.error(new IllegalStateException()) : saveState(gameState, redisKey))
                .flatMap(saved -> saved ? Mono.just(gameState.getRoomId()) : Mono.error(new IllegalStateException()));
    }

    public Mono<Long> save(GameState gameState) {
        String redisKey = GAME_STATE_KEY_PREFIX + gameState.getRoomId();
        return saveState(gameState, redisKey)
                .flatMap(saved -> saved ? Mono.just(gameState.getRoomId()) : Mono.error(new IllegalStateException()));
    }

    private Mono<Boolean> checkKeyExists(String redisKey) {
        return redisOps.hasKey(redisKey);
    }

    public Mono<Boolean> saveState(GameState gameState, String redisKey) {
        return redisOps.opsForValue().set(redisKey, gameState);
    }
}