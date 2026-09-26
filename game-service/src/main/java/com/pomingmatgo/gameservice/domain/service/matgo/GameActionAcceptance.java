package com.pomingmatgo.gameservice.domain.service.matgo;

import reactor.core.publisher.Mono;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

final class GameActionAcceptance {
    private GameActionAcceptance() {}

    // in-memory AOP가 제공한 수락 경계만 사용한다. Redis·직접 서비스 호출은 기존 취소 의미를 유지한다.
    static <T> Mono<T> beforeMutation(Supplier<Mono<T>> mutation) {
        return Mono.deferContextual(context -> {
            BooleanSupplier acceptance = context.getOrDefault(GameActionAcceptance.class, () -> true);
            return acceptance.getAsBoolean() ? Mono.defer(mutation) : Mono.empty();
        });
    }
}
