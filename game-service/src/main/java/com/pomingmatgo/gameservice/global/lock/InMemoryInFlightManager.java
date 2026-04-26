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
            // 만료 엔트리가 잔존한 경우에도 CAS로 교체. isSet 호출이 선행되지 않아도 영구 차단되지 않음
            while (true) {
                long now = System.nanoTime();
                long newExpiry = now + ttl.toNanos();
                Long existing = flags.get(key);
                if (existing == null) {
                    if (flags.putIfAbsent(key, newExpiry) == null) return true;
                } else if (now > existing) {
                    if (flags.replace(key, existing, newExpiry)) return true;
                } else {
                    return false;
                }
            }
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
