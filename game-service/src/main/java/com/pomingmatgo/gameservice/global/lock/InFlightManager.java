package com.pomingmatgo.gameservice.global.lock;

import reactor.core.publisher.Mono;

import java.time.Duration;

public interface InFlightManager {
    Mono<Boolean> trySetFlag(String key, Object value, Duration ttl);
    Mono<Boolean> isSet(String key);
    Mono<Void> deleteFlag(String key);
}
