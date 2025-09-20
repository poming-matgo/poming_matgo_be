package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.WebSocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.global.exception.dto.WebSocketErrorResDto;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.SYSTEM_ERROR;


@Component
@RequiredArgsConstructor
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final RoomService roomService;
    private final SessionManager sessionManager;
    private final WsRoomHandler wsRoomHandler;
    private final WsPreGameHandler wsPreGameHandler;

    private static final Map<String, Class<?>> typeMappings = new HashMap<>() {{
        put("LEADER_SELECTION", LeadSelectionReq.class);
    }};

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> handleMessage(message, session))
                .then();
    }

    private Mono<Void> handleMessage(WebSocketMessage message, WebSocketSession session) {
        return Mono.fromCallable(() -> objectMapper.readValue(message.getPayloadAsText(),
                        new TypeReference<RequestEvent<Object>>() {}))
                .flatMap(event -> {
                    Class<?> targetType = typeMappings.getOrDefault(event.getEventType().getSubType(), Object.class);

                    Object data = objectMapper.convertValue(event.getData(), targetType);

                    RequestEvent<Object> typedEvent = new RequestEvent<>();
                    typedEvent.setEventType(event.getEventType());
                    typedEvent.setPlayerNum(event.getPlayerNum());
                    typedEvent.setRoomId(event.getRoomId());
                    typedEvent.setData(data);

                    return processEvent(typedEvent, session);
                })
                .then();
    }

    private Mono<Void> processEvent(RequestEvent<?> event, WebSocketSession session) {
        long userId = event.getPlayerNum();
        long roomId = event.getRoomId();

        return roomService.getGameState(roomId)
                .flatMap(gameState -> determinePlayerNum(userId, gameState)
                        .flatMap(playerNum -> {
                            sessionManager.addPlayer(roomId, playerNum, session);
                            return routeEvent(event, gameState, playerNum);
                        }))
                .onErrorResume(error -> handleWebSocketError(session, error));
    }

    private Mono<Void> handleWebSocketError(WebSocketSession session, Throwable error) {
        WebSocketErrorResDto dto;

        if (error instanceof WebSocketBusinessException businessException) {
            dto = new WebSocketErrorResDto(businessException.getWebsocketErrorCode());
        } else {
            dto = new WebSocketErrorResDto(SYSTEM_ERROR);
        }

        try {
            String jsonMessage = objectMapper.writeValueAsString(dto);
            WebSocketMessage webSocketMessage = session.textMessage(jsonMessage);
            return session.send(Mono.just(webSocketMessage));
        } catch (IOException e) {
            return Mono.error(e);
        }
    }


    private Mono<Integer> determinePlayerNum(long userId, GameState gameState) {
        return Mono.justOrEmpty(gameState)
                .flatMap(gs -> {
                    if (userId == gs.getPlayer1Id()) {
                        return Mono.just(1);
                    } else if (userId == gs.getPlayer2Id()) {
                        return Mono.just(2);
                    } else {
                        return Mono.error(new WebSocketBusinessException(WebSocketErrorCode.NOT_IN_ROOM));
                    }
                });
    }

    private Mono<Void> routeEvent(RequestEvent<?> event, GameState gameState, int playerNum) {
        String eventType = event.getEventType().getType();
        if ("ROOM".equals(eventType)) {
            return wsRoomHandler.handleRoomEvent(event, gameState, playerNum);
        } else if ("PREGAME".equals(eventType)) {
            return wsPreGameHandler.handlePreGameEvent(event, gameState, playerNum);
        }
        return Mono.empty();
    }
}
