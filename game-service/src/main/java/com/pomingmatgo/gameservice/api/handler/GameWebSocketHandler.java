package com.pomingmatgo.gameservice.api.handler;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.domain.service.matgo.PrePlayService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final PrePlayService prePlayService;
    private final RoomService roomService;
    private final SessionManager sessionManager;
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> handleMessage(message, session))
                .then();
    }

    private Mono<Void> handleMessage(WebSocketMessage message, WebSocketSession session) {
        return Mono.fromCallable(() -> objectMapper.readValue(message.getPayloadAsText(), RequestEvent.class))
                .flatMap(event -> routeEvent(event, session))
                .then(Mono.empty());
    }

    private Mono<Void> routeEvent(RequestEvent event, WebSocketSession session) {
        long userId = event.getUserId();
        long roomId = event.getRoomId();

        return roomService.getGameState(roomId)
                .flatMap(gameState -> determinePlayerNum(userId, gameState)
                        .flatMap(playerNum -> {
                            sessionManager.addPlayer(roomId, playerNum, session);
                            return handleEventType(event, gameState, playerNum);
                        })
                        .onErrorResume(Exception.class, error ->
                                session.send(Mono.just(session.textMessage("ERROR")))
                                        .then()
                        )
                );
    }

    private Mono<Integer> determinePlayerNum(long userId, GameState gameState) {
        if (userId == gameState.getPlayer1Id()) return Mono.just(1);
        if (userId == gameState.getPlayer2Id()) return Mono.just(2);
        return Mono.error(new BusinessException(ErrorCode.NOT_IN_ROOM));
    }

    private Mono<Void> handleEventType(RequestEvent event, GameState gameState, int playerNum) {
        String eventType = event.getEventType().getSubType();
        if ("READY".equals(eventType)) {
            return roomService.ready(Mono.just(gameState), playerNum);
        }
        return Mono.empty();
    }
}
