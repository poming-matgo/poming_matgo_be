package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.List;

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
        long roomId = gameState.getRoomId();
        return gameService.submitCardEvent(roomId, player, (RequestEvent<NormalSubmitReq>) event)
                .flatMap(submittedCard -> {
                    Mono<Card> topCardMono = sendSubmitCardInfo(roomId, player, submittedCard)
                            .then(gameService.getTopCard(roomId));

                    return topCardMono.flatMap(topCard ->
                            sendTopCardInfo(roomId, player, topCard)
                                    .then(gameService.submitCard(gameState, submittedCard, topCard))
                    );
                })
                .flatMap(processCardResult -> {
                    System.out.println(player + " " + gameState.getCurrentPlayer() + " " + gameState.getOtherPlayer());
                    if(processCardResult.isChoiceRequired())
                        return sendChooseFloorCardMessage(roomId, player, processCardResult.getAcquiredCards());
                    if(processCardResult.isClaimOpponentPi())
                        return sendMovingCardMessage(roomId, player, gameState.getOtherPlayer(), processCardResult.getMoveCard())
                                .then(sendAcquiredCardMessage(roomId, player, processCardResult.getAcquiredCards()));
                    return sendAcquiredCardMessage(roomId, player, processCardResult.getAcquiredCards());
                });
    }

    private Mono<Void> handleFloorSelectEvent(RequestEvent<?> event, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        return gameService.selectFloorCard(gameState, player, (RequestEvent<NormalSubmitReq>) event)
                .flatMap(processCardResult -> {
                    if(processCardResult.isChoiceRequired())
                        return sendChooseFloorCardMessage(roomId, player, processCardResult.getAcquiredCards());
                    return sendAcquiredCardMessage(roomId, player, processCardResult.getAcquiredCards());
                });
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
