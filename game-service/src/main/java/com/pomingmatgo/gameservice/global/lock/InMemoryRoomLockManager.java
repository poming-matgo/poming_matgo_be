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

    private static final long ACQUIRE_TIMEOUT_MILLIS = 1000L;

    private final ConcurrentHashMap<Long, Semaphore> locks = new ConcurrentHashMap<>();

    @Override
    public <T> Mono<T> withLock(long roomId, Mono<T> task, Supplier<? extends RuntimeException> lockFailError) {
        Semaphore semaphore = locks.computeIfAbsent(roomId, k -> new Semaphore(1));

        return Mono.fromCallable(() -> semaphore.tryAcquire(ACQUIRE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
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
        // compute + tryAcquire 변형은 withLock의 (computeIfAbsent → tryAcquire) 2단계 사이에 끼어들어
        // 다른 스레드의 permit을 가로채 영구 timeout을 유발. 단순 remove로 회귀.
        // remove 후 새 withLock이 새 Semaphore를 받지만, gameOver→cleanup→재시작 흐름은 정상 게임에서 충돌 거의 없음
        return Mono.fromRunnable(() -> locks.remove(roomId));
    }
}
