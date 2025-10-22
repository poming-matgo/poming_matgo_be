package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_NOTHING;

@Component
@RequiredArgsConstructor
public class WsRoomHandler {
    private final MessageSender messageSender;
    private final RoomService roomService;
    private final PreGameService preGameService;
    private enum RoomEventType {
        CONNECT, READY, UNREADY
    }

    public Mono<Void> handleRoomEvent(RequestEvent<?> event, GameState gameState, Player player) {
        RoomEventType eventType;
        try {
            eventType = RoomEventType.valueOf(event.getEventType().getSubType());
        } catch (IllegalArgumentException e) {
            return Mono.error(new IllegalArgumentException("Unsupported event type: " + event.getEventType().getSubType()));
        }

        return switch (eventType) {
            case CONNECT -> handleConnectEvent(gameState.getRoomId(), player);
            case READY -> handleReadyEvent(gameState, player);
            case UNREADY -> handleUnreadyEvent(gameState, player);
        };
    }

    private Mono<Void> handleConnectEvent(long roomId, Player player) {
        return messageSender.sendMessageToAllUser(roomId, WebSocketResDto.of(player, "CONNECT", "접속했습니다."));
    }
    private Mono<Void> handleReadyEvent(GameState gameState, Player player) {
        return roomService.ready(gameState, player, true)
                .flatMap(updatedGameState ->
                        messageSender.sendMessageToAllUser(
                                        gameState.getRoomId(),
                                        WebSocketResDto.of(player, "READY", "Ready 했습니다.")
                                )
                                // 모든 유저 준비 완료 시 후속 처리
                                .then(checkAndProceedIfAllReady(updatedGameState))
                );
    }

    private Mono<Void> handleUnreadyEvent(GameState gameState, Player player) {
        return roomService.ready(gameState, player, false)
                .then(
                        messageSender.sendMessageToAllUser(
                                gameState.getRoomId(),
                                WebSocketResDto.of(player, "UNREADY", "Ready 취소 했습니다.")
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
                PLAYER_NOTHING,
                "START",
                "게임이 시작됐습니다."
        );
        return messageSender.sendMessageToAllUser(roomId, startDto);
    }
}
