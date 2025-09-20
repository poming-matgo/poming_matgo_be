package com.pomingmatgo.gameservice.api.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.WebSocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.api.response.websocket.LeadSelectionRes;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.global.exception.dto.WebSocketErrorResDto;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.SYSTEM_ERROR;


@Component
@RequiredArgsConstructor
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final PreGameService preGameService;
    private final RoomService roomService;
    private final SessionManager sessionManager;
    private Collection<WebSocketSession> allUser;

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
                            allUser = sessionManager.getAllUser(gameState.getRoomId());
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

    private <T> Mono<Void> sendMessageToSession(WebSocketSession session, WebSocketResDto<T> response) {
        try {
            String jsonMessage = objectMapper.writeValueAsString(response);
            WebSocketMessage webSocketMessage = session.textMessage(jsonMessage);
            return session.send(Mono.just(webSocketMessage));
        } catch (Exception e) {
            return Mono.empty(); //todo: 예외처리로직  추가해야함
        }
    }

    public <T> Mono<Void> sendMessageToUsers(Collection<WebSocketSession> users, WebSocketResDto<T> response) {
        return Flux.fromIterable(users)
                .flatMap(session -> sendMessageToSession(session, response))
                .then();
    }

    private Mono<Void> handleRoomEvent(RequestEvent<?> event, GameState gameState, int playerNum) {
        String eventType = event.getEventType().getSubType();
        if ("CONNECT".equals(eventType)) {
            return sendMessageToUsers(allUser, new WebSocketResDto<>(
                    playerNum,
                    "CONNECT",
                    "접속했습니다."
            ));
        }
        else if ("READY".equals(eventType)) {
            return roomService.ready(gameState, playerNum, true)
                    .flatMap(updatedGameState ->
                            sendMessageToUsers(
                                    allUser,
                                    new WebSocketResDto<>(playerNum, "READY", "Ready 했습니다.")
                            )
                                    .then(roomService.checkAllPlayersReady(Mono.just(updatedGameState)))
                                    .flatMap(allReady -> Boolean.TRUE.equals(allReady)
                                            ? handleAllReadyEvent()
                                            .then(Mono.defer(() -> preGameService.pickFiveCardsAndSave(updatedGameState.getRoomId())))
                                            : Mono.empty()
                                    )
                    );
        }
        else if ("UNREADY".equals(eventType)) {
            return roomService.ready(gameState, playerNum, true)
                    .then(
                            sendMessageToUsers(
                                    allUser,
                                    new WebSocketResDto<>(
                                            playerNum,
                                            "UNREADY",
                                            "Ready 취소 했습니다."
                                    )
                            )
                    )
                    .then(Mono.empty());
        }
        return Mono.empty();
    }

    private Mono<Void> handlePreGameEvent(RequestEvent<?> event, GameState gameState, int playerNum) {
        String eventType = event.getEventType().getSubType();
        long roomId = gameState.getRoomId();
        if("LEADER_SELECTION".equals(eventType)) {
            if (event.getData() instanceof LeadSelectionReq) {
                return preGameService.selectCard((RequestEvent<LeadSelectionReq>) event)
                        .then(
                                sendMessageToUsers(
                                        allUser,
                                        new WebSocketResDto<>(
                                                playerNum,
                                                "LEADER_SELECTION",
                                                "선두 플레이어 선택"
                                        )
                                )
                        )
                        .then(preGameService.isAllPlayerCardSelected(roomId))
                        .flatMap(allSelected -> {
                            if (Boolean.TRUE.equals(allSelected)) {
                                return handleAllSelectedEvent(roomId)
                                        .then(Mono.defer(() -> preGameService.distributeCards(roomId)))
                                        .flatMap(cards -> sendDistributedCardInfo(roomId, cards)
                                                .then(/*todo: announceTurnToAllPlayers*/));
                            }
                            return Mono.empty();
                        });
            }

        }

        return Mono.empty();
    }

    private Mono<Void> routeEvent(RequestEvent<?> event, GameState gameState, int playerNum) {
        String eventType = event.getEventType().getType();
        if("ROOM".equals(eventType)) {
            return handleRoomEvent(event, gameState, playerNum);
        }
        else if("PREGAME".equals(eventType)) {
            return handlePreGameEvent(event,gameState, playerNum);
        }
        return Mono.empty();
    }

    private Mono<Void> handleAllReadyEvent() {
        // 게임이 시작되었다는 메시지를 모든 사용자에게 전송
        WebSocketResDto<Void> startDto = new WebSocketResDto<>(
                0,
                "START",
                "게임이 시작됐습니다."
        );
        return sendMessageToUsers(allUser, startDto);
    }

    public Mono<Void> handleAllSelectedEvent(long roomId) {
        return preGameService.getLeadSelectionRes(roomId)
                .flatMap(leadSelectionRes -> {
                    WebSocketResDto<LeadSelectionRes> setLeadDto = new WebSocketResDto<>(
                            0,
                            "LEADER_SELECTION_RESULT",
                            "선을 정했습니다.",
                            leadSelectionRes
                    );
                    return sendMessageToUsers(allUser, setLeadDto);
                });
    }

    private Mono<Void> sendDistributedCardInfo(long roomId, InstalledCard installedCard) {
        WebSocketResDto<List<String>> ret1 = new WebSocketResDto<>(
                1,
                "DISTRIBUTE_CARD",
                "카드를 배분합니다.",
                installedCard.getPlayer1()
                        .stream()
                        .map(Enum::name)
                        .toList()
        );

        WebSocketResDto<List<String>> ret2 = new WebSocketResDto<>(
                2,
                "DISTRIBUTE_CARD",
                "카드를 배분합니다.",
                installedCard.getPlayer2()
                        .stream()
                        .map(Enum::name)
                        .toList()
        );

        WebSocketSession player1Session = sessionManager.getSession(roomId, 1);
        WebSocketSession player2Session = sessionManager.getSession(roomId, 2);

        return Mono.when(
                sendMessageToSession(player1Session, ret1),
                sendMessageToSession(player2Session, ret2)
        );
    }

    /*private Mono<Void> announceTurnToAllPlayers() {

    }*/
}
