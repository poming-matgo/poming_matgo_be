package com.pomingmatgo.gameservice.global.lock;

import lombok.RequiredArgsConstructor;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Profile("redis")
@Component
@RequiredArgsConstructor
public class RedisInFlightManager implements InFlightManager {

    private final RedissonReactiveClient redissonClient;

    @Override
    public Mono<Boolean> trySetFlag(String key, String token, Duration ttl) {
        return redissonClient.getBucket(key).setIfAbsent(token, ttl);
    }

    @Override
    public Mono<Boolean> isSet(String key) {
        return redissonClient.getBucket(key).isExists();
    }

    @Override
    public Mono<Void> deleteFlag(String key, String token) {
        // 소유 토큰이 일치할 때만 삭제 (compareAndSet(expect, null) == 조건부 DEL, Lua 기반 원자 연산)
        return redissonClient.getBucket(key).compareAndSet(token, null).then();
    }
}
