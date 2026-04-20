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
    public Mono<Boolean> trySetFlag(String key, Object value, Duration ttl) {
        return redissonClient.getBucket(key).setIfAbsent(value, ttl);
    }

    @Override
    public Mono<Boolean> isSet(String key) {
        return redissonClient.getBucket(key).isExists();
    }

    @Override
    public Mono<Void> deleteFlag(String key) {
        return redissonClient.getBucket(key).delete().then();
    }
}
