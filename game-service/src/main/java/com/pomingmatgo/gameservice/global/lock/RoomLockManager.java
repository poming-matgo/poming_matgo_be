package com.pomingmatgo.gameservice.global.lock;

import reactor.core.publisher.Mono;

import java.util.function.Supplier;

public interface RoomLockManager {
    <T> Mono<T> withLock(long roomId, Mono<T> task, Supplier<? extends RuntimeException> lockFailError);
}
