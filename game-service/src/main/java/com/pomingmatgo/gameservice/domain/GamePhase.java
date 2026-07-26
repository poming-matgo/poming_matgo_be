package com.pomingmatgo.gameservice.domain;

import lombok.Getter;

// turnStepOrder = 한 턴 안에서 행동 대기 단계가 진행되는 순서 (행동 대기가 아니면 -1).
// AutoPlayScheduler의 타이머 교체 판정과 게임 액션 판정(isPlayerActionPhase)이 공유한다.
// 10 간격 — 새 대기 phase는 자기 순서 값만 선언하면 두 판정에 자동 편입된다
@Getter
public enum GamePhase {
    NONE(-1),
    DETERMINING_STARTING_PLAYER(-1),
    IN_PROGRESS(10),
    END(-1),
    AWAITING_FLOOR_CARD_CHOICE(20),
    AWAITING_GO_STOP_CHOICE(30);

    private final int turnStepOrder;

    GamePhase(int turnStepOrder) {
        this.turnStepOrder = turnStepOrder;
    }

    /** 현재 플레이어의 행동을 기다리는 phase인지 — 자동플레이 타이머 대상이기도 하다 */
    public boolean isPlayerActionPhase() {
        return turnStepOrder >= 0;
    }
}
