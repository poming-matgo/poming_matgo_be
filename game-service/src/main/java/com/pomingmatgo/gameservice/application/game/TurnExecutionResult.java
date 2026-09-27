package com.pomingmatgo.gameservice.application.game;

import com.pomingmatgo.gameservice.domain.rule.ProcessCardResult;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.card.Card;

public record TurnExecutionResult(
        Card submittedCard,
        Card topCard,
        ProcessCardResult cardResult,
        GameState updatedGameState
) {
    public boolean isChoiceRequired() {
        return cardResult.isChoiceRequired();
    }
}
