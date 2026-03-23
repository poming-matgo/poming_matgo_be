package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.category.EventCategory;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.request.websocket.JoinRoomReq;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.global.exception.dto.WebSocketErrorResDto;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.*;


@Component
@RequiredArgsConstructor
@Slf4j
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final RoomService roomService;
    private final SessionManager sessionManager;
    private final WsRoomHandler wsRoomHandler;
    private final WsPreGameHandler wsPreGameHandler;
    private final WsGameHandler wsGameHandler;
    private final MessageSender messageSender;

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> handleMessage(message, session))
                .then();
    }

    private Mono<Void> handleMessage(WebSocketMessage message, WebSocketSession session) {
        return Mono.fromCallable(() -> {
                    JsonNode rootNode = objectMapper.readTree(message.getPayloadAsText());
                    String subTypeStr = rootNode.path("eventType").path("subType").asText();

                    SubCategory subType = SubCategory.from(subTypeStr);
                    JavaType type = objectMapper.getTypeFactory().constructParametricType(RequestEvent.class, subType.getPayloadClass());

                    return (RequestEvent<?>) objectMapper.convertValue(rootNode, type);
                })
                .flatMap(event -> processEvent(event, session))
                .onErrorResume(error -> handleWebSocketError(session, error));
    }

    private Mono<Void> processEvent(RequestEvent<?> event, WebSocketSession session) {
        if (SubCategory.CONNECT.name().equals(event.getEventType().getSubType())) {
            return handleJoinRoom(event.as(), session);
        }

        return sessionManager.getPlayerContext(session)
                .flatMap(context -> roomService.getGameState(context.roomId())
                        .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_IN_ROOM)))
                        .flatMap(gameState -> routeEvent(event, gameState, Player.fromNumber(context.playerNum()))));
    }

    private Mono<Void> handleJoinRoom(RequestEvent<JoinRoomReq> event, WebSocketSession session) {
        return sessionManager.getPlayerContext(session)
                .hasElement()
                .flatMap(isJoined -> isJoined
                        ? Mono.error(new WebSocketBusinessException(ALREADY_JOIN))
                        : processJoinRoomLogic(event.getData(), session));
    }

    private Mono<Void> processJoinRoomLogic(JoinRoomReq payload, WebSocketSession session) {
        long userId = payload.userId();
        long roomId = payload.roomId();

        return roomService.getGameState(roomId)
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_EXISTED_ROOM)))
                .flatMap(gameState -> Mono.fromCallable(() -> gameState.getPlayerType(userId)))
                .flatMap(player -> sessionManager.addPlayer(roomId, player, userId, session).thenReturn(player))
                .flatMap(player -> messageSender.sendMessageToAllUser(
                        roomId, WebSocketResDto.of(player, "CONNECT", "접속했습니다."))
                );
    }

    private Mono<Void> handleWebSocketError(WebSocketSession session, Throwable error) {
        boolean isSystemError = true;
        WebSocketErrorCode errorCode = SYSTEM_ERROR;

        if (error instanceof WebSocketBusinessException wbe) {
            errorCode = wbe.getWebsocketErrorCode();
            isSystemError = (errorCode == SYSTEM_ERROR);
        }

        if (isSystemError) {
            log.error("WebSocket system error occurred in session [{}].", session.getId(), error);
        }

        WebSocketErrorResDto errorDto = new WebSocketErrorResDto(errorCode);

        return Mono.fromCallable(() -> objectMapper.writeValueAsString(errorDto))
                .map(session::textMessage)
                .flatMap(message -> session.send(Mono.just(message)));
    }

    private Mono<Void> routeEvent(RequestEvent<?> event, GameState gameState, Player player) {
        EventCategory eventType = EventCategory.valueOf(event.getEventType().getType());

        return switch (eventType) {
            case ROOM -> wsRoomHandler.handleRoomEvent(event, gameState, player);
            case PREGAME -> wsPreGameHandler.handlePreGameEvent(event, gameState, player);
            case GAME -> wsGameHandler.handleGameEvent(event, gameState, player);
            default -> Mono.empty();
        };
    }
}
