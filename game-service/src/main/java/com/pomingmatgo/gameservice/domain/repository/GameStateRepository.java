package com.pomingmatgo.gameservice.domain.repository;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
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
                .flatMap(exists -> {
                    if (Boolean.TRUE.equals(exists)) {
                        return Mono.error(new BusinessException(ErrorCode.ALREADY_EXISTED_ROOM));
                    }
                    return saveState(gameState, redisKey);
                })
                .flatMap(saved -> {
                    if (Boolean.TRUE.equals(saved)) {
                        return Mono.just(gameState.getRoomId());
                    }
                    return Mono.error(new BusinessException(ErrorCode.SYSTEM_ERROR));
                });
    }

    public Mono<Long> save(GameState gameState) {
        String redisKey = GAME_STATE_KEY_PREFIX + gameState.getRoomId();
        return saveState(gameState, redisKey)
                .flatMap(saved -> {
                    if (Boolean.TRUE.equals(saved)) {
                        return Mono.just(gameState.getRoomId());
                    }
                    return Mono.error(new BusinessException(ErrorCode.SYSTEM_ERROR));
                });
    }

    private Mono<Boolean> checkKeyExists(String redisKey) {
        return redisOps.hasKey(redisKey);
    }

    public Mono<Boolean> saveState(GameState gameState, String redisKey) {
        return redisOps.opsForValue().set(redisKey, gameState);
    }
}