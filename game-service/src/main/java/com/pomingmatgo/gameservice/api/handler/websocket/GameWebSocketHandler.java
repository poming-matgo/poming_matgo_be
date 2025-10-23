package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.JoinRoomReq;
import com.pomingmatgo.gameservice.api.request.websocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.dto.WebSocketErrorResDto;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.NOT_IN_ROOM;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.SYSTEM_ERROR;


@Component
@RequiredArgsConstructor
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final RoomService roomService;
    private final SessionManager sessionManager;
    private final WsRoomHandler wsRoomHandler;
    private final WsPreGameHandler wsPreGameHandler;
    private final WsGameHandler wsGameHandler;
    private final MessageSender messageSender;

    private static final Map<String, Class<?>> typeMappings = new HashMap<>() {{
        put("LEADER_SELECTION", LeadSelectionReq.class);
        put("NORMAL_SUBMIT", NormalSubmitReq.class);
        put("CONNECT", JoinRoomReq.class);
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
                    Object typedData = objectMapper.convertValue(event.getData(), targetType);
                    return processEvent(event.withData(typedData), session);
                });
    }

    private Mono<Void> processEvent(RequestEvent<?> event, WebSocketSession session) {
        if ("CONNECT".equals(event.getEventType().getSubType())) {
            return handleJoinRoom(event, session);
        }

        return sessionManager.getPlayerContext(session)
                .flatMap(context -> roomService.getGameState(context.roomId())
                        .flatMap(gameState -> routeEvent(event, gameState, Player.fromNumber(context.playerNum())))
                .switchIfEmpty(handleWebSocketError(session, new WebSocketBusinessException(NOT_IN_ROOM)))
                .onErrorResume(error -> handleWebSocketError(session, error)));
    }

    private Mono<Void> handleJoinRoom(RequestEvent<?> event, WebSocketSession session) {
        JoinRoomReq payload = (JoinRoomReq) event.getData();
        long userId = payload.getUserId();
        long roomId = payload.getRoomId();

        return roomService.getGameState(roomId)
                .flatMap(gameState -> determinePlayerNum(userId, gameState))
                .flatMap(player ->
                        sessionManager.addPlayer(roomId, player, userId, session)
                                .thenReturn(player)
                )
                .flatMap(player ->
                        messageSender.sendMessageToAllUser(roomId, WebSocketResDto.of(player, "CONNECT", "접속했습니다."))
                )
                .onErrorResume(error -> handleWebSocketError(session, error));
    }

    private Mono<Void> handleWebSocketError(WebSocketSession session, Throwable error) {
        WebSocketErrorResDto dto = (error instanceof WebSocketBusinessException businessException)
                ? new WebSocketErrorResDto(businessException.getWebsocketErrorCode())
                : new WebSocketErrorResDto(SYSTEM_ERROR);

        return Mono.fromCallable(() -> objectMapper.writeValueAsString(dto))
                .map(session::textMessage)
                .flatMap(message -> session.send(Mono.just(message)));
    }


    private Mono<Player> determinePlayerNum(long userId, GameState gameState) {
        return Mono.fromCallable(() -> gameState.getPlayerNumber(userId))
                .map(Player::fromNumber);
    }

    private Mono<Void> routeEvent(RequestEvent<?> event, GameState gameState, Player player) {
        String eventType = event.getEventType().getType();
        return switch (eventType) {
            case "ROOM" -> wsRoomHandler.handleRoomEvent(event, gameState, player);
            case "PREGAME" -> wsPreGameHandler.handlePreGameEvent(event, gameState, player);
            case "GAME" -> wsGameHandler.handleGameEvent(event, gameState, player);
            default -> Mono.empty();
        };
    }
}
