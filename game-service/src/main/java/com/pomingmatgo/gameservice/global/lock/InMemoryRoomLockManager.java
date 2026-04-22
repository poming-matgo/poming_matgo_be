package com.pomingmatgo.gameservice.global.lock;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Profile("in-memory")
@Component
public class InMemoryRoomLockManager implements RoomLockManager {

    private final ConcurrentHashMap<Long, Semaphore> locks = new ConcurrentHashMap<>();

    @Override
    public <T> Mono<T> withLock(long roomId, Mono<T> task, Supplier<? extends RuntimeException> lockFailError) {
        Semaphore semaphore = locks.computeIfAbsent(roomId, k -> new Semaphore(1));

        return Mono.fromCallable(() -> semaphore.tryAcquire(5000, TimeUnit.MILLISECONDS))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(acquired -> {
                    if (!acquired) return Mono.error(lockFailError.get());
                    return Mono.usingWhen(
                            Mono.just(semaphore),
                            s -> task,
                            s -> Mono.fromRunnable(s::release),
                            (s, err) -> Mono.fromRunnable(s::release),
                            s -> Mono.fromRunnable(s::release)
                    );
                });
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> locks.remove(roomId));
    }
}
