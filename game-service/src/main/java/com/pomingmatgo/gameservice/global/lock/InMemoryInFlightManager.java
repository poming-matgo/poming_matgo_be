package com.pomingmatgo.gameservice.global.lock;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

@Profile("in-memory")
@Component
public class InMemoryInFlightManager implements InFlightManager {

    private final ConcurrentHashMap<String, Long> flags = new ConcurrentHashMap<>();

    @Override
    public Mono<Boolean> trySetFlag(String key, Object value, Duration ttl) {
        return Mono.fromCallable(() -> {
            long expiry = System.nanoTime() + ttl.toNanos();
            return flags.putIfAbsent(key, expiry) == null;
        });
    }

    @Override
    public Mono<Boolean> isSet(String key) {
        return Mono.fromCallable(() -> {
            Long expiry = flags.get(key);
            if (expiry == null) return false;
            if (System.nanoTime() > expiry) {
                flags.remove(key, expiry);
                return false;
            }
            return true;
        });
    }

    @Override
    public Mono<Void> deleteFlag(String key) {
        return Mono.fromRunnable(() -> flags.remove(key));
    }
}
