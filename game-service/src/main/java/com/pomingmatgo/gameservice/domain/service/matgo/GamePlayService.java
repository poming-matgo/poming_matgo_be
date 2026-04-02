package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.GoStopReq;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
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

    @GameLock(key = "'game:' + #roomId + ':' + #gameState.round + ':' + #gameState.turn")
    public Mono<TurnExecutionResult> executeNormalSubmit(long roomId, GameState gameState, Player player, int cardIdx) {
        Mono<Card> submittedCardMono = gameService.submitCardEvent(roomId, player, cardIdx);
        Mono<Card> topCardMono = gameService.getTopCard(roomId);

        return Mono.zip(submittedCardMono, topCardMono)
                .flatMap(tuple -> {
                    Card submittedCard = tuple.getT1();
                    Card topCard = tuple.getT2();

                    return gameService.submitCard(gameState, submittedCard, topCard)
                            .flatMap(processResult -> buildNormalSubmitResult(
                                    roomId, gameState, submittedCard, topCard, processResult
                            ));
                });
    }

    @GameLock(key = "'game:' + #roomId + ':' + #gameState.round + ':' + #gameState.turn")
    public Mono<FloorSelectionResult> executeFloorSelection(long roomId, GameState gameState, Player player, RequestEvent<NormalSubmitReq> event) {
        return gameService.selectFloorCard(gameState, player, event)
                .flatMap(result -> {
                    if (result.isChoiceRequired()) {
                        return Mono.just(new FloorSelectionResult(result, gameState, true));
                    }

                    return applyTurnEffects(roomId, gameState, result)
                            .then(gameService.calculateAndApplyScores(roomId, gameState))
                            .map(nextState -> new FloorSelectionResult(result, nextState, false));
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
        return gameService.setNextTurn(gameState)
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

    public Mono<GameState> executeGoStop(GameState gameState, Player player, RequestEvent<GoStopReq> event) {
        boolean go = event.getData().go();

        return go ? gameService.executeGoStop(gameState, player)
                .flatMap(this::proceedToNextTurn) : Mono.empty();
    }
}
