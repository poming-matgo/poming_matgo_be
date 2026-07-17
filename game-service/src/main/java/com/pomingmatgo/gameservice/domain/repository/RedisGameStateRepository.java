package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Profile("redis")
@Repository
public class RedisGameStateRepository implements GameStateRepository {
    private final ReactiveRedisOperations<String, GameState> redisOps;

    public RedisGameStateRepository(@Qualifier("gameStateRedisTemplate") ReactiveRedisOperations<String, GameState> redisOps) {
        this.redisOps = redisOps;
    }

    private static final String GAME_STATE_KEY_FORMAT = "game:%d:state";


    private String generateKey(long roomId) {
        return String.format(GAME_STATE_KEY_FORMAT, roomId);
    }

    public Mono<GameState> findById(long roomId) {
        String redisKey = generateKey(roomId);

        return redisOps.opsForValue().get(redisKey);
    }

    public Mono<Long> create(GameState gameState) {
        String redisKey = generateKey(gameState.getRoomId());

        return redisOps.opsForValue()
                .setIfAbsent(redisKey, gameState)
                .flatMap(wasSet -> {
                    if (Boolean.TRUE.equals(wasSet)) {
                        return Mono.just(gameState.getRoomId());
                    } else {
                        return Mono.error(new BusinessException(ErrorCode.ALREADY_EXISTED_ROOM));
                    }
                });
    }

    public Mono<Long> delete(long roomId) {
        String redisKey = generateKey(roomId);
        return redisOps.hasKey(redisKey)
                .flatMap(exists -> {
                    if (Boolean.TRUE.equals(exists)) {
                        return redisOps.delete(redisKey);
                    }
                    return Mono.just(roomId);
                });
    }

    public Mono<Long> save(GameState gameState) {
        String redisKey = generateKey(gameState.getRoomId());

        return redisOps.opsForValue()
                .setIfPresent(redisKey, gameState)
                .filter(Boolean::booleanValue)
                .map(b -> gameState.getRoomId())
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.SYSTEM_ERROR)));
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return redisOps.delete(generateKey(roomId)).then();
    }
}
