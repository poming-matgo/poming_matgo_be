package com.pomingmatgo.gameservice.api.handler;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
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

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.SYSTEM_ERROR;


@Component
@RequiredArgsConstructor
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final PreGameService preGameService;
    private final RoomService roomService;
    private final SessionManager sessionManager;
    private Collection<WebSocketSession> allUser;
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> handleMessage(message, session))
                .then();
    }

    private Mono<Void> handleMessage(WebSocketMessage message, WebSocketSession session) {
        return Mono.fromCallable(() -> objectMapper.readValue(message.getPayloadAsText(), RequestEvent.class))
                .flatMap(event -> processEvent(event, session))
                .then();
    }

    private Mono<Void> processEvent(RequestEvent<?> event, WebSocketSession session) {
        long userId = event.getUserId();
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
                        return Mono.error(new BusinessException(ErrorCode.NOT_IN_ROOM));
                    }
                });
    }

    public <T> Mono<Void> sendMessageToUsers(Collection<WebSocketSession> users, WebSocketResDto<T> response) {
        return Flux.fromIterable(users)
                .flatMap(session -> {
                    try {
                        String jsonMessage = objectMapper.writeValueAsString(response);
                        WebSocketMessage webSocketMessage = session.textMessage(jsonMessage);
                        return session.send(Mono.just(webSocketMessage));
                    } catch (Exception e) {
                        return Mono.empty(); //todo: 에러처리로직 추가해야함
                    }
                })
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
            return roomService.ready(Mono.just(gameState), playerNum, true)
                    .flatMap(updatedGameState ->
                            sendMessageToUsers(
                                    allUser,
                                    new WebSocketResDto<>(playerNum, "READY", "Ready 했습니다.")
                            )
                                    .then(roomService.checkAllPlayersReady(Mono.just(updatedGameState)))
                                    .flatMap(allReady -> Boolean.TRUE.equals(allReady)
                                            ? handleAllReadyEvent(allUser)
                                            .then(Mono.defer(() -> preGameService.pickFiveCardsAndSave(updatedGameState.getRoomId())))
                                            : Mono.empty()
                                    )
                    );
        }
        else if ("UNREADY".equals(eventType)) {
            return roomService.ready(Mono.just(gameState), playerNum, true)
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
        if("LEADER_SELECTION".equals(eventType)) {

        }
        return Mono.empty();
    }

    private Mono<Void> routeEvent(RequestEvent event, GameState gameState, int playerNum) {
        String eventType = event.getEventType().getType();
        allUser = sessionManager.getAllUser(gameState.getRoomId());
        if("ROOM".equals(eventType)) {
            return handleRoomEvent(event, gameState, playerNum);
        }
        else if("PREGAME".equals(eventType)) {
            return handlePreGameEvent(event,gameState, playerNum);
        }
        return Mono.empty();
    }

    private Mono<Void> handleAllReadyEvent(Collection<WebSocketSession> allUsers) {
        // 게임이 시작되었다는 메시지를 모든 사용자에게 전송
        WebSocketResDto<Void> startDto = new WebSocketResDto<>(
                0,
                "START",
                "게임이 시작됐습니다."
        );
        return sendMessageToUsers(allUsers, startDto);
    }
}
