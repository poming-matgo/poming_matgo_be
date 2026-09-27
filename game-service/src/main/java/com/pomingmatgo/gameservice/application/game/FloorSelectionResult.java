package com.pomingmatgo.gameservice.application.game;

import com.pomingmatgo.gameservice.domain.rule.ProcessCardResult;

import com.pomingmatgo.gameservice.domain.GameState;

public record FloorSelectionResult(
        ProcessCardResult cardResult,
        GameState updatedGameState
) {
    public boolean isChoiceRequired() {
        return cardResult.isChoiceRequired();
    }
}
