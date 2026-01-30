package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;

public record FloorSelectionResult(
        ProcessCardResult cardResult,
        GameState updatedGameState,
        boolean isChoiceRequired
) {}