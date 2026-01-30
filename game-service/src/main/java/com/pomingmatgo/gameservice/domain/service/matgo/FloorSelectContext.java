package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;

public record FloorSelectContext(
        ProcessCardResult result,   // 카드 처리 결과 (획득 카드 등)
        GameState updatedGameState, // 업데이트된 게임 상태 (턴 정보 등)
        boolean isChoiceRequired    // 선택이 필요한지 여부
) {}