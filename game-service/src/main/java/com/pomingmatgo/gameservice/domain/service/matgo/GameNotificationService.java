package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.response.websocket.GameMessageSender;
import com.pomingmatgo.gameservice.api.response.websocket.ScoreInfoRes;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class GameNotificationService {
    private final GameMessageSender gameMessageSender;
    private final GamePlayService gamePlayService;

    public Mono<GameState> broadcastTurnResult(long roomId, Player player, GameState gameState, ProcessCardResult result, long remainingMs) {
        Mono<Void> sendMoveCards = Flux.fromIterable(result.getMoveCards())
                .concatMap(card -> gameMessageSender.sendMovingCardMessage(roomId, player, gameState.getOtherPlayer(), card))
                .then();

        Mono<Void> sendAcquired = Mono.empty();
        if (!result.getSpecialEvents().contains(SpecialEvent.PPEOK)) {
            sendAcquired = gameMessageSender.sendAcquiredCardMessage(roomId, player, result.getAcquiredCards());
        }

        Mono<Void> sendSpecial = Flux.fromIterable(result.getSpecialEvents())
                .concatMap(event -> gameMessageSender.sendSpecialEventMessageIfNeeded(roomId, player, event))
                .then();

        return sendMoveCards
                .then(sendAcquired)
                .then(sendSpecial)
                .then(sendScoreInfo(gameState))
                .then(Mono.defer(() -> {
                    if (gameState.isLastTurn()) {
                        return processGameOver(gameState, player);
                    }
                    if (gamePlayService.canGoStop(gameState, player)) {
                        // 마지막 라운드엔 GO 선택지가 없으므로 곧바로 게임 종료
                        if (gameState.isFinalRound()) {
                            return processGameOver(gameState, player);
                        }
                        // phase를 저장해 두면 선택 요청 검증과 자동플레이 타이머가 이 상태를 근거로 동작한다.
                        // 타이머 등록은 반환된 phase를 보고 TurnFlowService가 수행 (이 턴의 낡은 타이머는 원자적으로 교체됨)
                        return gamePlayService.enterGoStopChoice(gameState)
                                .delayUntil(awaitingState -> gameMessageSender.sendGoStopChoiceMessage(awaitingState, player));
                    } else {
                        return gamePlayService.proceedToNextTurn(gameState)
                                .flatMap(nextState -> gameMessageSender.sendTurnInfo(nextState, remainingMs)
                                .thenReturn(nextState));
                    }
                }));
    }

    private Mono<GameState> processGameOver(GameState gameState, Player player) {
        return gamePlayService.gameOver(gameState)
                .delayUntil(finalState -> gameMessageSender.sendGameOverMessage(finalState, player));
    }

    private Mono<Void> sendScoreInfo(GameState gameState) {
        ScoreInfoRes scoreInfoRes = ScoreInfoRes.from(gameState);
        return gameMessageSender.sendScoreInfo(gameState.getRoomId(), scoreInfoRes);
    }
}
