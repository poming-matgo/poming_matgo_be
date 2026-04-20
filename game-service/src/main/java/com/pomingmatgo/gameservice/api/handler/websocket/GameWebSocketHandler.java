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
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static com.pomingmatgo.gameservice.domain.GamePhase.IN_PROGRESS;
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
    private final InFlightManager inFlightManager;

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> {
                    long arrivalTime = System.currentTimeMillis();
                    return handleMessage(message, session, arrivalTime);
                })
                .then();
    }

    private Mono<Void> handleMessage(WebSocketMessage message, WebSocketSession session, long arrivalTime) {
        return Mono.fromCallable(() -> {
                    JsonNode rootNode = objectMapper.readTree(message.getPayloadAsText());
                    String subTypeStr = rootNode.path("eventType").path("subType").asText();

                    SubCategory subType = SubCategory.from(subTypeStr);
                    JavaType type = objectMapper.getTypeFactory().constructParametricType(RequestEvent.class, subType.getPayloadClass());

                    RequestEvent<?> event = objectMapper.convertValue(rootNode, type);

                    event.setArrivalTime(arrivalTime);
                    return event;
                })
                .flatMap(event -> processEvent(event, session))
                .onErrorResume(error -> handleWebSocketError(session, error));
    }

    private Mono<Void> processEvent(RequestEvent<?> event, WebSocketSession session) {
        if (SubCategory.CONNECT.name().equals(event.getEventType().getSubType())) {
            return handleJoinRoom((RequestEvent<JoinRoomReq>) event, session);
        }

        return sessionManager.getPlayerContext(session)
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_IN_ROOM)))
                .flatMap(context -> {
                    long roomId = context.roomId();
                    Player player = Player.fromNumber(context.playerNum());

                    return roomService.getGameState(roomId)
                            .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_EXISTED_ROOM)))
                            .flatMap(gameState -> {
                                if (isGameAction(event, gameState, player)) {
                                    String flagKey = "IN_FLIGHT:ROOM:" + roomId + ":PLAYER:" + player.getNumber();

                                    return inFlightManager.trySetFlag(flagKey, event.getArrivalTime(), Duration.ofSeconds(3))
                                            .flatMap(isSet -> {
                                                if (!isSet) return Mono.error(new WebSocketBusinessException(TOO_MANY_REQUESTS));
                                                return Mono.usingWhen(
                                                        Mono.just(flagKey),
                                                        key -> routeEvent(event, gameState, player),
                                                        key -> inFlightManager.deleteFlag(key)
                                                );
                                            });
                                }

                                return routeEvent(event, gameState, player);
                            });
                });
    }

    private boolean isGameAction(RequestEvent<?> event, GameState gameState, Player player) {
        SubCategory eventType = SubCategory.from(event.getEventType().getSubType());

        boolean cond1 =  eventType == SubCategory.NORMAL_SUBMIT ||
               eventType == SubCategory.FLOOR_SELECT ||
               eventType == SubCategory.GO_STOP_CHOICE;

        boolean cond2 = gameState.getPhase() == IN_PROGRESS &&
                player.equals(gameState.getCurrentPlayer());

        return cond1 && cond2;
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
