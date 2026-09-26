package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;

@FunctionalInterface
public interface GameActionCompletion {
    // 타이머·송신이 필요 없는 직접 상태 실행에서만 명시적으로 사용한다.
    GameActionCompletion NONE = state -> {};

    /** 상태 저장 후, 락 해제 전에 실행한다. 예외는 액션 실패로 전파한다. */
    void onStateSaved(GameState state);
}
