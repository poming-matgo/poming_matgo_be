package com.pomingmatgo.gameservice.global.lock;

import reactor.core.publisher.Mono;

public interface GameLockCleaner {
    Mono<Void> cleanup(long roomId);
}
