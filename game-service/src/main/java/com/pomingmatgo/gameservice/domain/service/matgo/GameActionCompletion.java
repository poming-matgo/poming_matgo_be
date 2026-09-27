package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface GameActionCompletion {
    // 타이머·재시작 후처리가 필요 없는 직접 상태 실행에서만 명시적으로 사용한다.
    GameActionCompletion NONE = state -> Mono.empty();

    /** 상태 저장 후 반환한 작업의 완료까지 락을 유지한다. 오류는 액션 실패로 전파한다. */
    Mono<Void> onStateSaved(GameState state);
}
