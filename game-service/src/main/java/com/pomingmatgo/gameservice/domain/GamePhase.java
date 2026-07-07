package com.pomingmatgo.gameservice.domain;

import lombok.Getter;

/**
 * 게임 phase. 플레이어 행동 대기 phase는 turnStepOrder를 갖는다.
 *
 * turnStepOrder: 한 턴(round, turn) 안에서 행동 대기 단계가 진행되는 순서.
 * AutoPlayScheduler의 TurnStep 교체 판정(낡은 앞 단계 타이머 등록이 뒤 단계 타이머를
 * 파괴하지 못하게)과 GameWebSocketHandler의 게임 액션 판정(isPlayerActionPhase)이 공유한다.
 * 값은 10 간격 — 새 대기 phase(예: 흔들기)는 자기 순서에 맞는 값만 선언하면 두 판정에 자동 편입된다.
 * 행동 대기가 아닌 phase는 -1.
 */
@Getter
public enum GamePhase {
    NONE(-1),
    // 선 플레이어 정하는 중
    DETERMINING_STARTING_PLAYER(-1),
    IN_PROGRESS(10),
    END(-1),
    // 특별한 대기 상태
    AWAITING_FLOOR_CARD_CHOICE(20),
    AWAITING_GO_STOP_CHOICE(30);

    private final int turnStepOrder;

    GamePhase(int turnStepOrder) {
        this.turnStepOrder = turnStepOrder;
    }

    /** 현재 플레이어의 게임 액션(제출/선택)을 기다리는 phase인지 — 자동플레이 타이머 대상이기도 하다 */
    public boolean isPlayerActionPhase() {
        return turnStepOrder >= 0;
    }
}
