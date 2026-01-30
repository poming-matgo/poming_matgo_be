package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.card.Card;

public record TurnResultContext(
        Card submittedCard,
        Card topCard,
        ProcessCardResult processCardResult,
        GameState newGameState
) {}