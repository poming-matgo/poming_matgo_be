package com.pomingmatgo.gameservice.infrastructure.lock;

import reactor.core.publisher.Mono;
import java.util.function.Supplier;

public interface GameLockCleaner {
    Mono<Void> cleanup(long roomId);

    // Redis는 기존 실행 방식을 유지한다. in-memory 구현만 액션과 정리를 조정한다.
    default Mono<Void> withCleanup(long roomId, Supplier<Mono<Void>> operation) {
        return Mono.defer(operation);
    }

    // in-memory는 데이터 삭제부터 재생성까지 같은 실행을 소유한다. Redis는 기존 취소 의미를 유지한다.
    default Mono<Void> withRestart(long roomId, Supplier<Mono<Void>> operation) {
        return Mono.defer(operation);
    }
}
