package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.api.response.websocket.GameMessageSender;
import com.pomingmatgo.gameservice.api.response.websocket.ScoreInfoRes;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.service.matgo.*;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.NOT_YOUR_TURN;

@Component
@RequiredArgsConstructor
public class WsGameHandler {
    private final GameService gameService;
    private final GameMessageSender gameMessageSender;

    private enum GameEventType {
        NORMAL_SUBMIT,
        FLOOR_SELECT
    }


    public Mono<Void> handleGameEvent(RequestEvent<?> event, GameState gameState, Player player) {
        if(player != gameState.getCurrentPlayer()) {
            throw new WebSocketBusinessException(NOT_YOUR_TURN);
        }
        WsGameHandler.GameEventType eventType;
        try {
            eventType = WsGameHandler.GameEventType.valueOf(event.getEventType().getSubType());
        } catch (IllegalArgumentException e) {
            return Mono.error(new IllegalArgumentException("Unsupported event type: " + event.getEventType().getSubType()));
        }

        return switch (eventType) {
            case NORMAL_SUBMIT -> handleNormalSubmitEvent(event, gameState, player);
            case FLOOR_SELECT->  handleFloorSelectEvent(event, gameState, player);
        };
    }

    public Mono<Void> handleNormalSubmitEvent(RequestEvent<?> event, GameState gameState, Player player) {
        return executeTurnStateUpdate(event, gameState, player)
                .flatMap(context -> {
                    long roomId = gameState.getRoomId();

                    Mono<Void> sendSubmitMono = gameMessageSender.sendSubmitCardInfo(roomId, player, context.submittedCard());

                    Mono<Void> sendTopCardMono = gameMessageSender.sendTopCardInfo(roomId, player, context.topCard());

                    Mono<Void> handleResultMono = handleCardSubmissionResult(
                            context.processCardResult(),
                            context.newGameState(),
                            player
                    );

                    return sendSubmitMono
                            .then(sendTopCardMono)
                            .then(handleResultMono);
                });
    }

    @GameLock(key = "'game:' + #gameState.roomId")
    private Mono<TurnResultContext> executeTurnStateUpdate(RequestEvent<?> event, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();

        Mono<Card> submittedCardMono = gameService.submitCardEvent(roomId, player, (RequestEvent<NormalSubmitReq>) event);
        Mono<Card> topCardMono = gameService.getTopCard(roomId);

        return Mono.zip(submittedCardMono, topCardMono)
                .flatMap(tuple -> {
                    Card submittedCard = tuple.getT1();
                    Card topCard = tuple.getT2();

                    return gameService.submitCard(gameState, submittedCard, topCard)
                            .flatMap(processCardResult ->
                                    applyTurnResult(roomId, gameState, processCardResult)
                                            .then(gameService.calculateAndApplyScores(roomId, gameState))
                                            .map(newGs -> new TurnResultContext(submittedCard, topCard, processCardResult, newGs))
                            );
                });
    }

    //getAndLossCard가 이미 확정된 상황에서 redis에 반영하기만 하는 코드
    private Mono<Void> applyTurnResult(long roomId, GameState gameState, ProcessCardResult processCardResult) {
        if (processCardResult.isChoiceRequired()) {
            return Mono.empty();
        }

        Mono<Void> precedingOperation = Mono.empty();
        if (processCardResult.isClaimOpponentPi()) {
            precedingOperation = gameService.loseCard(roomId, gameState.getOtherPlayer(), processCardResult.getMoveCard());
        }
        List<Card> acquiredCards = processCardResult.getAcquiredCards();
        return precedingOperation
                .then(gameService.acquireCards(roomId, gameState.getCurrentPlayer(), acquiredCards));
    }

    private Mono<Void> handleCardSubmissionResult(ProcessCardResult processCardResult, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();

        if (processCardResult.isChoiceRequired()) {
            return gameMessageSender.sendChooseFloorCardMessage(roomId, player, processCardResult.getAcquiredCards());
        }

        Mono<Void> messagingMono;
        if (processCardResult.getSpecialEvent()== SpecialEvent.PPEOK) {
            messagingMono = Mono.empty();
        } else if (processCardResult.isClaimOpponentPi()) {
            messagingMono = gameMessageSender.sendMovingCardMessage(roomId, player, gameState.getOtherPlayer(), processCardResult.getMoveCard())
                    .then(gameMessageSender.sendAcquiredCardMessage(roomId, player, processCardResult.getAcquiredCards()));
        } else {
            messagingMono = gameMessageSender.sendAcquiredCardMessage(roomId, player, processCardResult.getAcquiredCards());
        }

        return messagingMono.then(gameMessageSender.sendSpecialEventMessageIfNeeded(roomId, player, processCardResult))
                .then(proceedToNextTurn(gameState));
    }

    @GameLock(key = "'game:' + #gameState.roomId")
    private Mono<Void> handleFloorSelectEvent(RequestEvent<?> event, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        return gameService.selectFloorCard(gameState, player, (RequestEvent<NormalSubmitReq>) event)
                .flatMap(result -> {
                    if (result.isChoiceRequired()) {
                        return gameMessageSender.sendChooseFloorCardMessage(roomId, player, result.getAcquiredCards());
                    }

                    return applyTurnResult(roomId, gameState, result)
                            .then(gameMessageSender.sendAcquiredCardMessage(roomId, player, result.getAcquiredCards()))
                            .then(proceedToNextTurn(gameState));
                });
    }

    private Mono<Void> proceedToNextTurn(GameState gameState) {
        return gameService.setNextTurn(gameState)
                .flatMap(updatedGameState -> {
                    Mono<Void> saveStateMono = gameService.setGameInProgress(updatedGameState);

                    ScoreInfoRes scoreInfoRes = ScoreInfoRes.from(updatedGameState);
                    Mono<Void> sendTurnInfoMono = gameMessageSender.sendTurnInfo(updatedGameState);
                    Mono<Void> sendScoreInfoMono = gameMessageSender.sendScoreInfo(updatedGameState.getRoomId(), scoreInfoRes);

                    return saveStateMono.then(Mono.when(sendTurnInfoMono, sendScoreInfoMono));
                });
    }
}
