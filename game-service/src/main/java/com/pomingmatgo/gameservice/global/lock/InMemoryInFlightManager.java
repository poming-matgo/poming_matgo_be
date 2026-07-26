package com.pomingmatgo.gameservice.global.lock;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

@Profile("in-memory")
@Component
public class InMemoryInFlightManager implements InFlightManager {

    private record Flag(String token, long expiryNanos) {}

    private final ConcurrentHashMap<String, Flag> flags = new ConcurrentHashMap<>();

    @Override
    public Mono<Boolean> trySetFlag(String key, String token, Duration ttl) {
        return Mono.fromCallable(() -> {
            // 만료 엔트리가 잔존한 경우에도 CAS로 교체. isSet 호출이 선행되지 않아도 영구 차단되지 않음
            while (true) {
                long now = System.nanoTime();
                Flag next = new Flag(token, now + ttl.toNanos());
                Flag existing = flags.get(key);
                if (existing == null) {
                    if (flags.putIfAbsent(key, next) == null) return true;
                } else if (now > existing.expiryNanos()) {
                    if (flags.replace(key, existing, next)) return true;
                } else {
                    return false;
                }
            }
        });
    }

    @Override
    public Mono<Boolean> isSet(String key) {
        return Mono.fromCallable(() -> {
            Flag flag = flags.get(key);
            if (flag == null) return false;
            if (System.nanoTime() > flag.expiryNanos()) {
                flags.remove(key, flag);
                return false;
            }
            return true;
        });
    }

    @Override
    public Mono<Void> deleteFlag(String key, String token) {
        return Mono.fromRunnable(() -> {
            // 소유 토큰이 일치할 때만 삭제 — remove(key, value)는 원자 연산이라 그 사이 교체됐어도 no-op
            Flag current = flags.get(key);
            if (current != null && current.token().equals(token)) {
                flags.remove(key, current);
            }
        });
    }
}
