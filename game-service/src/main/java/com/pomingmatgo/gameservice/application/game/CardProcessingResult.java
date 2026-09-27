package com.pomingmatgo.gameservice.application.game;

import com.pomingmatgo.gameservice.domain.rule.ProcessCardResult;

import com.pomingmatgo.gameservice.domain.GameState;

// 카드 처리 직후의 상태이며, 턴 정산과 다음 턴 전환은 GamePlayService가 이어서 수행한다.
public record CardProcessingResult(
        ProcessCardResult cardResult,
        GameState updatedGameState
) {}
