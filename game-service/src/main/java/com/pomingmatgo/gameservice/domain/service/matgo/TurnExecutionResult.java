package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.card.Card;

public record TurnExecutionResult(
        Card submittedCard,
        Card topCard,
        ProcessCardResult cardResult,
        GameState updatedGameState,
        boolean isChoiceRequired
) {}