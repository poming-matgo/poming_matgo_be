package com.pomingmatgo.gameservice.infrastructure.session;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/** 등록마다 새로 만들며, 조회 경계와 서버 송신 완료를 구분한다. */
public final class SnapshotDelivery {
    private volatile boolean captured;
    private final Sinks.One<Boolean> delivered = Sinks.one();

    public void captured() {
        captured = true;
    }

    public void complete(boolean success) {
        delivered.tryEmitValue(success);
    }

    // 게임 락 안에서 평가한다. 조회 전에 완료한 액션은 나중에 조회에 포함된다.
    public Mono<Boolean> capturePermission() {
        return captured ? delivered.asMono() : Mono.just(false);
    }
}
