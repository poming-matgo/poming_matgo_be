package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class WsRoomHandler {
    private final MessageSender messageSender;
    private final RoomService roomService;
    private final PreGameService preGameService;
    private enum RoomEventType {
        CONNECT, READY, UNREADY
    }

    public Mono<Void> handleRoomEvent(RequestEvent<?> event, GameState gameState, int playerNum) {
        RoomEventType eventType;
        try {
            eventType = RoomEventType.valueOf(event.getEventType().getSubType());
        } catch (IllegalArgumentException e) {
            return Mono.error(new IllegalArgumentException("Unsupported event type: " + event.getEventType().getSubType()));
        }

        return switch (eventType) {
            case CONNECT -> handleConnectEvent(gameState.getRoomId(), playerNum);
            case READY -> handleReadyEvent(gameState, playerNum);
            case UNREADY -> handleUnreadyEvent(gameState, playerNum);
        };
    }

    private Mono<Void> handleConnectEvent(long roomId, int playerNum) {
        return messageSender.sendMessageToAllUser(roomId, WebSocketResDto.of(playerNum, "CONNECT", "접속했습니다."));
    }
    private Mono<Void> handleReadyEvent(GameState gameState, int playerNum) {
        return roomService.ready(gameState, playerNum, true)
                .flatMap(updatedGameState ->
                        messageSender.sendMessageToAllUser(
                                        gameState.getRoomId(),
                                        WebSocketResDto.of(playerNum, "READY", "Ready 했습니다.")
                                )
                                // 모든 유저 준비 완료 시 후속 처리
                                .then(checkAndProceedIfAllReady(updatedGameState))
                );
    }

    private Mono<Void> handleUnreadyEvent(GameState gameState, int playerNum) {
        return roomService.ready(gameState, playerNum, false)
                .then(
                        messageSender.sendMessageToAllUser(
                                gameState.getRoomId(),
                                WebSocketResDto.of(playerNum, "UNREADY", "Ready 취소 했습니다.")
                        )
                );
    }

    private Mono<Void> checkAndProceedIfAllReady(GameState updatedGameState) {
        return Mono.defer(() -> {
            if (roomService.checkAllPlayersReady(updatedGameState)) {
                return handleAllReadyEvent(updatedGameState.getRoomId())
                        .then(preGameService.pickFiveCardsAndSave(updatedGameState.getRoomId()));
            }
            return Mono.empty();
        });
    }

    private Mono<Void> handleAllReadyEvent(long roomId) {
        WebSocketResDto<Void> startDto = new WebSocketResDto<>(
                0,
                "START",
                "게임이 시작됐습니다."
        );
        return messageSender.sendMessageToAllUser(roomId, startDto);
    }
}
