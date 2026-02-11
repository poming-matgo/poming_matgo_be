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

import static com.pomingmatgo.gameservice.domain.GamePhase.*;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.NOT_YOUR_TURN;

@Component
@RequiredArgsConstructor
public class WsGameHandler {

    private final GamePlayService gamePlayService;
    private final GameMessageSender gameMessageSender;

    private enum GameEventType {
        NORMAL_SUBMIT,
        FLOOR_SELECT;

        public static GameEventType from(String subType) {
            try {
                return valueOf(subType);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported event type: " + subType);
            }
        }
    }

    public Mono<Void> handleGameEvent(RequestEvent<?> event, GameState gameState, Player player) {
        if (!player.equals(gameState.getCurrentPlayer())) {
            return Mono.error(new WebSocketBusinessException(NOT_YOUR_TURN));
        }

        GameEventType eventType = GameEventType.from(event.getEventType().getSubType());

        @SuppressWarnings("unchecked")
        RequestEvent<NormalSubmitReq> submitEvent = (RequestEvent<NormalSubmitReq>) event;

        return switch (eventType) {
            case NORMAL_SUBMIT -> handleNormalSubmit(submitEvent, gameState, player);
            case FLOOR_SELECT -> handleFloorSelect(submitEvent, gameState, player);
        };
    }

    private Mono<Void> handleNormalSubmit(RequestEvent<NormalSubmitReq> event, GameState gameState, Player player) {
        if(gameState.getPhase() != IN_PROGRESS) {
            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
        }

        long roomId = gameState.getRoomId();

        return gamePlayService.executeNormalSubmit(roomId, gameState, player, event)
                .flatMap(ctx -> {
                    Mono<Void> sendInfos = Mono.when(
                            gameMessageSender.sendSubmitCardInfo(roomId, player, ctx.submittedCard()),
                            gameMessageSender.sendTopCardInfo(roomId, player, ctx.topCard())
                    );

                    Mono<Void> handleResult;
                    if (ctx.isChoiceRequired()) {
                        handleResult = gameMessageSender.sendChooseFloorCardMessage(roomId, player, ctx.cardResult().getAcquiredCards());
                    } else {
                        handleResult = broadcastTurnResult(roomId, player, ctx.updatedGameState(), ctx.cardResult());
                    }

                    return sendInfos.then(handleResult);
                });
    }

    private Mono<Void> handleFloorSelect(RequestEvent<NormalSubmitReq> event, GameState gameState, Player player) {
        if(gameState.getPhase() != AWAITING_FLOOR_CARD_CHOICE) {
            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
        }
        long roomId = gameState.getRoomId();

        return gamePlayService.executeFloorSelection(roomId, gameState, player, event)
                .flatMap(ctx -> {
                    if (ctx.isChoiceRequired()) {
                        return gameMessageSender.sendChooseFloorCardMessage(roomId, player, ctx.cardResult().getAcquiredCards());
                    }

                    Mono<Void> sendAcquired = gameMessageSender.sendAcquiredCardMessage(roomId, player, ctx.cardResult().getAcquiredCards());
                    Mono<Void> broadcastNextTurn = broadcastNextTurnInfo(ctx.updatedGameState());

                    return sendAcquired.then(broadcastNextTurn);
                });
    }


    private Mono<Void> broadcastTurnResult(long roomId, Player player, GameState gameState, ProcessCardResult result) {
        Mono<Void> sendAcquired = Mono.empty();

        if (result.isClaimOpponentPi()) {
            sendAcquired = gameMessageSender.sendMovingCardMessage(roomId, player, gameState.getOtherPlayer(), result.getMoveCard());
        }

        if (result.getSpecialEvent() != SpecialEvent.PPEOK) {
            sendAcquired = sendAcquired.then(gameMessageSender.sendAcquiredCardMessage(roomId, player, result.getAcquiredCards()));
        }

        Mono<Void> sendSpecial = gameMessageSender.sendSpecialEventMessageIfNeeded(roomId, player, result);

        return sendAcquired
                .then(sendSpecial)
                .then(gamePlayService.proceedToNextTurn(gameState))
                .flatMap(this::broadcastNextTurnInfo);
    }

    private Mono<Void> broadcastNextTurnInfo(GameState nextState) {
        ScoreInfoRes scoreInfoRes = ScoreInfoRes.from(nextState);
        return Mono.when(
                gameMessageSender.sendTurnInfo(nextState),
                gameMessageSender.sendScoreInfo(nextState.getRoomId(), scoreInfoRes)
        );
    }
}
