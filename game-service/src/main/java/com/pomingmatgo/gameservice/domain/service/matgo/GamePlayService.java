package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.GoStopReq;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
            return Mono.zip(gameService.submitCardEvent(roomId, player, cardIdx), gameService.getTopCard(roomId))
                    .flatMap(tuple -> {
                        Card submittedCard = tuple.getT1();
                        Card topCard = tuple.getT2();

                        return gameService.submitCard(gameState, submittedCard, topCard)
                                .flatMap(processResult -> buildNormalSubmitResult(
                                        roomId, gameState, submittedCard, topCard, processResult
                                ));
                    });
        });
    }

    @GameLock(key = "'game:' + #roomId + ':' + #gameState.round + ':' + #gameState.currentTurn")
    public Mono<FloorSelectionResult> executeFloorSelection(long roomId, GameState gameState, Player player, RequestEvent<NormalSubmitReq> event, Runnable onLockAcquired) {
        return Mono.defer(() -> {
            if (onLockAcquired != null) {
                onLockAcquired.run();
            }
            return gameService.selectFloorCard(gameState, player, event)
                    .flatMap(result -> {
                        if (result.isChoiceRequired()) {
                            return Mono.just(new FloorSelectionResult(result, gameState, true));
                        }

                        return applyTurnEffects(roomId, gameState, result)
                                .then(gameService.calculateAndApplyScores(roomId, gameState))
                                .map(nextState -> new FloorSelectionResult(result, nextState, false));
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
        return Mono.just(gameState)
                .map(gameState::setNextTurn)
                .flatMap(gameService::setGameInProgress);
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

    public Mono<GameState> executeGoStop(GameState gameState, Player player, RequestEvent<GoStopReq> event, Runnable onLockAcquired) {
        return Mono.defer(() -> {
            if (onLockAcquired != null) {
                onLockAcquired.run();
            }

            boolean go = event.getData().go();

            return go ? gameService.executeGoStop(gameState, player)
                    .flatMap(this::proceedToNextTurn) : Mono.empty();
        });
    }

    public Mono<GameState> gameOver(GameState gameState, Player winner) {
        return gameService.gameOver(gameState, winner);
    }
}
