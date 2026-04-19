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

    public Mono<GameState> broadcastTurnResult(long roomId, Player player, GameState gameState, ProcessCardResult result, Runnable onLockAcquired) {
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
                    if (gamePlayService.canGoStop(gameState, player)) {
                        if (onLockAcquired != null) {
                            onLockAcquired.run();
                        }
                        if(gameState.getRound() == 10) {
                            return gamePlayService.gameOver(gameState, player)
                                    .flatMap(finalState -> gameMessageSender.sendGameOverMessage(finalState, player)
                                    .thenReturn(finalState));
                        }
                        return gameMessageSender.sendGoStopChoiceMessage(gameState, player)
                                .thenReturn(gameState);

                    } else {
                        return gamePlayService.proceedToNextTurn(gameState)
                                .flatMap(nextState ->gameMessageSender.sendTurnInfo(nextState)
                                .thenReturn(nextState));
                    }
                }));
    }

    private Mono<Void> sendScoreInfo(GameState gameState) {
        ScoreInfoRes scoreInfoRes = ScoreInfoRes.from(gameState);
        return gameMessageSender.sendScoreInfo(gameState.getRoomId(), scoreInfoRes);
    }

    public Mono<Void> broadcastNextTurnInfo(GameState nextState) {
        ScoreInfoRes scoreInfoRes = ScoreInfoRes.from(nextState);
        return Mono.when(
                gameMessageSender.sendTurnInfo(nextState),
                gameMessageSender.sendScoreInfo(nextState.getRoomId(), scoreInfoRes)
        );
    }
}
