package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.NOT_YOUR_TURN;

@Service
@RequiredArgsConstructor
public class GamePlayService {
    private final GameService gameService;

    @GameLock(key = "'game:' + #roomId + ':' + #gameState.round + ':' + #gameState.currentTurn")
    public Mono<TurnExecutionResult> executeNormalSubmit(long roomId, GameState gameState, Player player, int cardIdx, Runnable onLockAcquired) {
        return Mono.defer(() -> {
            if (onLockAcquired != null) {
                onLockAcquired.run();
            }
            return gameService.findGameState(roomId)
                    .flatMap(freshState -> {
                        if (freshState.getPhase() != GamePhase.IN_PROGRESS) {
                            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
                        }
                        if (!player.equals(freshState.getCurrentPlayer())) {
                            return Mono.error(new WebSocketBusinessException(NOT_YOUR_TURN));
                        }
                        return Mono.zip(gameService.submitCardEvent(roomId, player, cardIdx), gameService.getTopCard(roomId))
                                .flatMap(tuple -> {
                                    Card submittedCard = tuple.getT1();
                                    Card topCard = tuple.getT2();
                                    return gameService.submitCard(freshState, submittedCard, topCard)
                                            .flatMap(processResult -> buildNormalSubmitResult(
                                                    roomId, freshState, submittedCard, topCard, processResult
                                            ));
                                });
                    });
        });
    }

    @GameLock(key = "'floor:' + #roomId + ':' + #gameState.round + ':' + #gameState.currentTurn")
    public Mono<FloorSelectionResult> executeFloorSelection(long roomId, GameState gameState, Player player, int cardIdx, Runnable onLockAcquired) {
        return Mono.defer(() -> {
            if (onLockAcquired != null) {
                onLockAcquired.run();
            }
            return gameService.findGameState(roomId)
                    .flatMap(freshState -> {
                        if (freshState.getPhase() != GamePhase.AWAITING_FLOOR_CARD_CHOICE) {
                            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
                        }
                        return gameService.selectFloorCard(freshState, player, cardIdx)
                                .flatMap(result -> {
                                    if (result.isChoiceRequired()) {
                                        return Mono.just(new FloorSelectionResult(result, freshState, true));
                                    }
                                    return applyTurnEffects(roomId, freshState, result)
                                            .then(gameService.calculateAndApplyScores(roomId, freshState))
                                            .map(nextState -> new FloorSelectionResult(result, nextState, false));
                                });
                    });
        });
    }

    private Mono<TurnExecutionResult> buildNormalSubmitResult(long roomId, GameState gameState, Card submittedCard, Card topCard, ProcessCardResult processResult) {
        if (processResult.isChoiceRequired()) {
            return Mono.just(new TurnExecutionResult(submittedCard, topCard, processResult, gameState, true));
        }

        return applyTurnEffects(roomId, gameState, processResult)
                .then(gameService.calculateAndApplyScores(roomId, gameState))
                .map(newGs -> new TurnExecutionResult(submittedCard, topCard, processResult, newGs, false));
    }

    public Mono<GameState> proceedToNextTurn(GameState gameState) {
        return gameService.setGameInProgress(gameState.setNextTurn());
    }

    private Mono<Void> applyTurnEffects(long roomId, GameState gameState, ProcessCardResult result) {
        if (result.isChoiceRequired()) return Mono.empty();
        Mono<Void> precedingOperation = Flux.fromIterable(result.getMoveCards())
                .concatMap(card -> gameService.loseCard(roomId, gameState.getOtherPlayer(), card))
                .then();

        return precedingOperation
                .then(gameService.acquireCards(roomId, gameState.getCurrentPlayer(), result.getAcquiredCards()));
    }

    public boolean canGoStop(GameState gameState, Player player) {
        return gameState.canGoStop(player);
    }

    public Mono<GameState> enterGoStopChoice(GameState gameState) {
        return gameService.enterGoStopChoice(gameState);
    }

    @GameLock(key = "'game:' + #roomId + ':' + #gameState.round + ':' + #gameState.currentTurn")
    public Mono<GameState> executeGoStop(long roomId, GameState gameState, Player player, boolean go, Runnable onLockAcquired) {
        return Mono.defer(() -> {
            if (onLockAcquired != null) {
                onLockAcquired.run();
            }
            return gameService.findGameState(roomId)
                    .flatMap(freshState -> {
                        if (freshState.getPhase() != GamePhase.AWAITING_GO_STOP_CHOICE) {
                            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
                        }
                        if (!player.equals(freshState.getCurrentPlayer())) {
                            return Mono.error(new WebSocketBusinessException(NOT_YOUR_TURN));
                        }
                        return go ? gameService.executeGoStop(freshState, player)
                                .flatMap(this::proceedToNextTurn)
                                : Mono.just(freshState.toBuilder().phase(GamePhase.END).build());
                    });
        });
    }

    public Mono<GameState> gameOver(GameState gameState) {
        return gameService.gameOver(gameState);
    }
}
