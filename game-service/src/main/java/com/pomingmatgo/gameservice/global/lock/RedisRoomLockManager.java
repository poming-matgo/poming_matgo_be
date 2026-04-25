package com.pomingmatgo.gameservice.global.lock;

import lombok.RequiredArgsConstructor;
import org.redisson.api.RLockReactive;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Profile("redis")
@Component
@RequiredArgsConstructor
public class RedisRoomLockManager implements RoomLockManager {

    private static final long ACQUIRE_WAIT_MILLIS = 1000L;
    private static final long LEASE_TIME_MILLIS = -1L;

    private final RedissonReactiveClient redissonClient;

    @Override
    public <T> Mono<T> withLock(long roomId, Mono<T> task, Supplier<? extends RuntimeException> lockFailError) {
        RLockReactive lock = redissonClient.getLock("READY_LOCK:ROOM:" + roomId);
        long executionId = UUID.randomUUID().getMostSignificantBits();

        return Mono.usingWhen(
                lock.tryLock(ACQUIRE_WAIT_MILLIS, LEASE_TIME_MILLIS, TimeUnit.MILLISECONDS, executionId)
                        .flatMap(acquired -> acquired
                                ? Mono.just(lock)
                                : Mono.error(lockFailError.get())),
                l -> task,
                l -> unlock(l, executionId),
                (l, err) -> unlock(l, executionId),
                l -> unlock(l, executionId)
        );
    }

    private Mono<Void> unlock(RLockReactive lock, long executionId) {
        return lock.unlock(executionId).then().onErrorResume(e -> Mono.empty());
    }
}
