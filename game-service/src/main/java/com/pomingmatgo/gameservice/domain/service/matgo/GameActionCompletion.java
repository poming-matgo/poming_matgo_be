package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

record GameActionCompletion(Consumer<GameState> onStateSaved) {
    // AOP가 소유한 구독 안에서 기존 타이머 취소와 다음 타이머 등록을 락 해제 전에 끝낸다.
    static <T> Mono<T> complete(T result, GameState state, Runnable onActionSucceeded) {
        return Mono.deferContextual(context -> {
            if (onActionSucceeded != null) onActionSucceeded.run();
            context.<GameActionCompletion>getOrEmpty(GameActionCompletion.class)
                    .ifPresent(completion -> completion.onStateSaved.accept(state));
            return Mono.just(result);
        });
    }
}
