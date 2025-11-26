package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.api.response.websocket.AnnounceRoundRes;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import com.pomingmatgo.gameservice.domain.service.matgo.ProcessCardResult;
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
    private final MessageSender messageSender;
    private final SessionManager sessionManager;

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
                    Mono<Card> topCardMono = sendSubmitCardInfo(roomId, player, submittedCard)
                            .then(gameService.getTopCard(roomId));

                    return topCardMono.flatMap(topCard ->
                            sendTopCardInfo(roomId, player, topCard)
                                    .then(gameService.submitCard(gameState, submittedCard, topCard))
                    );
                });
    }

    private Mono<Void> handleCardSubmissionResult(ProcessCardResult processCardResult, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();

        if (processCardResult.isChoiceRequired()) {
            return sendChooseFloorCardMessage(roomId, player, processCardResult.getAcquiredCards());
        }

        Mono<Void> messagingMono;
        if (processCardResult.isPpeok()) {
            messagingMono = sendPpeokMessage(roomId, player);
        } else if (processCardResult.isClaimOpponentPi()) {
            messagingMono = sendMovingCardMessage(roomId, player, gameState.getOtherPlayer(), processCardResult.getMoveCard())
                    .then(sendAcquiredCardMessage(roomId, player, processCardResult.getAcquiredCards()));
        } else {
            messagingMono = sendAcquiredCardMessage(roomId, player, processCardResult.getAcquiredCards());
        }

        return messagingMono.then(proceedToNextTurn(gameState));
    }


    private Mono<Void> handleFloorSelectEvent(RequestEvent<?> event, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        return gameService.selectFloorCard(gameState, player, (RequestEvent<NormalSubmitReq>) event)
                .flatMap(processCardResult -> {
                    Mono<Void> messagingMono = processCardResult.isChoiceRequired()
                            ? sendChooseFloorCardMessage(roomId, player, processCardResult.getAcquiredCards())
                            : sendAcquiredCardMessage(roomId, player, processCardResult.getAcquiredCards());

                    if (processCardResult.isChoiceRequired()) {
                        return messagingMono;
                    }

                    return messagingMono.then(proceedToNextTurn(gameState));
                });
    }

    private Mono<Void> proceedToNextTurn(GameState gameState) {
        return gameService.setNextTurn(gameState)
                .flatMap(this::sendTurnInfo);
    }

    private Mono<Void> sendSubmitCardInfo(long roomId, Player player, Card card) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, "SUBMIT_CARD", "카드 제출", card)
        );
    }
    
    private Mono<Void> sendTopCardInfo(long roomId, Player player, Card card) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, "SUBMIT_CARD", "상단 카드 정보", card)
        );
    }

    private Mono<Void> sendAcquiredCardMessage(long roomId, Player player, List<Card> card) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, "ACQUIRED_CARD", "카드 획득", card)
        );
    }

    private Mono<Void> sendPpeokMessage(long roomId, Player player) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, "PPEOK", "뻑!")
        );
    }

    private Mono<Void> sendTurnInfo(GameState gameState) {
        AnnounceRoundRes res = new AnnounceRoundRes(
                gameState.getRound(),
                gameState.getCurrentTurn(),
                gameState.getCurrentPlayer()
        );
        return messageSender.sendMessageToAllUser(gameState.getRoomId(),
                WebSocketResDto.of(PLAYER_NOTHING, "ANNOUNCE_TURN_INFORMATION", "턴을 알립니다.", res));
    }

    private Mono<Void> sendChooseFloorCardMessage(long roomId, Player player, List<Card> card) {
        WebSocketSession session = sessionManager.getSession(roomId, player.getNumber());
        return messageSender.sendMessageToSession(
                session,
                WebSocketResDto.of(player, "CHOOSE_FLOOR_CARD", "바닥 카드 선택", card));
    }

    //뺏는것과 빼앗는것 구분 필요
    private Mono<Void> sendMovingCardMessage(long roomId, Player player, Player otherPlayer, Card card) {
        WebSocketSession session = sessionManager.getSession(roomId, player.getNumber());
        WebSocketSession otherSession = sessionManager.getSession(roomId, otherPlayer.getNumber());
        return messageSender.sendMessageToSession(
                session,
                WebSocketResDto.of(player, "OPPONENT_PI_CLAIMED", "상대방의 카드를 빼았습니다.", card)).then(
                messageSender.sendMessageToSession(
                        otherSession,
                        WebSocketResDto.of(otherPlayer, "OPPONENT_PI_CLAIMED", "상대방이 피를 뺏습니다.", card)
        ));
    }
}
