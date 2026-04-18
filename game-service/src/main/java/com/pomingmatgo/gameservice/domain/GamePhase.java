package com.pomingmatgo.gameservice.domain;

public enum GamePhase {
    NONE,
    // 선 플레이어 정하는 중
    DETERMINING_STARTING_PLAYER,
    IN_PROGRESS,
    END,
    // 특별한 대기 상태
    AWAITING_FLOOR_CARD_CHOICE
}