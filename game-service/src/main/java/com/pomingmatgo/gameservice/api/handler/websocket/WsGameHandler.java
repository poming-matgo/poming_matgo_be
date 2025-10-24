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
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@RequiredArgsConstructor
public class WsGameHandler {
    private final GameService gameService;
    private final MessageSender messageSender;
    private final SessionManager sessionManager;

    private enum GameEventType {
        NORMAL_SUBMIT
    }

    public Mono<Void> handleGameEvent(RequestEvent<?> event, GameState gameState, Player player) {
        WsGameHandler.GameEventType eventType;
        try {
            eventType = WsGameHandler.GameEventType.valueOf(event.getEventType().getSubType());
        } catch (IllegalArgumentException e) {
            return Mono.error(new IllegalArgumentException("Unsupported event type: " + event.getEventType().getSubType()));
        }

        return switch (eventType) {
            case NORMAL_SUBMIT -> handleNormalSubmitEvent(event, gameState.getRoomId(), player);
        };
    }

    private Mono<Void> handleNormalSubmitEvent(RequestEvent<?> event, long roomId, Player player) {
        return gameService.submitCardEvent(roomId, player, (RequestEvent<NormalSubmitReq>) event)
                .flatMap(submittedCard -> {
                    Mono<Card> topCardMono = sendSubmitCardInfo(roomId, player, submittedCard)
                            .then(gameService.getTopCard(roomId));

                    return topCardMono.flatMap(topCard ->
                            sendTopCardInfo(roomId, player, topCard)
                                    .then(gameService.submitCard(roomId, submittedCard, topCard))
                    );
                })
                .flatMap(cards -> sendAcquiredCardMessage(roomId, player, cards));
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
}
