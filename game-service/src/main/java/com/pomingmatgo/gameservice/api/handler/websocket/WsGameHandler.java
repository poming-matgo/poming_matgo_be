package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.api.response.websocket.AnnounceRoundRes;
import com.pomingmatgo.gameservice.api.response.websocket.GameMessageSender;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import com.pomingmatgo.gameservice.domain.service.matgo.ProcessCardResult;
import com.pomingmatgo.gameservice.domain.service.matgo.SpecialEvent;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_NOTHING;
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

    private Mono<Void> handleNormalSubmitEvent(RequestEvent<?> event, GameState gameState, Player player) {
        return processCardSubmission(event, gameState, player)
                .flatMap(processCardResult -> handleCardSubmissionResult(processCardResult, gameState, player));
    }


    private Mono<ProcessCardResult> processCardSubmission(RequestEvent<?> event, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        return gameService.submitCardEvent(roomId, player, (RequestEvent<NormalSubmitReq>) event)
                .flatMap(submittedCard -> {
                    Mono<Card> topCardMono = gameMessageSender.sendSubmitCardInfo(roomId, player, submittedCard)
                            .then(gameService.getTopCard(roomId));

                    return topCardMono.flatMap(topCard ->
                            gameMessageSender.sendTopCardInfo(roomId, player, topCard)
                                    .then(gameService.submitCard(gameState, submittedCard, topCard))
                    );
                });
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


    private Mono<Void> handleFloorSelectEvent(RequestEvent<?> event, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        return gameService.selectFloorCard(gameState, player, (RequestEvent<NormalSubmitReq>) event)
                .flatMap(processCardResult -> {
                    Mono<Void> messagingMono = processCardResult.isChoiceRequired()
                            ? gameMessageSender.sendChooseFloorCardMessage(roomId, player, processCardResult.getAcquiredCards())
                            : gameMessageSender.sendAcquiredCardMessage(roomId, player, processCardResult.getAcquiredCards());

                    if (processCardResult.isChoiceRequired()) {
                        return messagingMono;
                    }

                    return messagingMono.then(proceedToNextTurn(gameState));
                });
    }

    private Mono<Void> proceedToNextTurn(GameState gameState) {
        return gameService.setNextTurn(gameState)
                .flatMap(gameMessageSender::sendTurnInfo);
    }
}
