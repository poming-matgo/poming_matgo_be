package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.domain.GameState;
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
    public Mono<Void> handleGameEvent(RequestEvent<?> event, GameState gameState, int playerNum) {
        String eventType = event.getEventType().getSubType();
        long roomId = gameState.getRoomId();
        if("NORMAL_SUBMIT".equals(eventType)) {
            if (event.getData() instanceof NormalSubmitReq) {
                return gameService.submitCardEvent(roomId, (RequestEvent<NormalSubmitReq>)event)
                        .collectList()
                        .flatMap(card -> sendAcquiredCardMessage(roomId, playerNum, card));
            }
        }
        return Mono.empty();
    }

    private Mono<Void> sendAcquiredCardMessage(long roomId, int playerNum, List<Card> card) {
        return messageSender.sendMessageToAllUser(
                roomId,
                new WebSocketResDto<>(playerNum,
                        "ACQUIRED_CARD",
                        "카드 획득",
                        card)
        );
    }
}
