package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.application.room.RoomReadyService;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.messaging.ResponseEvent;
import com.pomingmatgo.gameservice.infrastructure.messaging.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_NOTHING;

@Component
@RequiredArgsConstructor
@Slf4j
public class WsRoomHandler {
    private final MessageSender messageSender;
    private final RoomReadyService readyService;


    public Mono<Void> handleRoomEvent(RequestEvent<?> event, GameState gameState, Player player) {
        return switch (event.getSubCategory()) {
            case READY -> handleReadyEvent(gameState, player);
            case UNREADY -> handleUnreadyEvent(gameState, player);
            default -> Mono.error(new IllegalStateException("처리기가 없는 ROOM 이벤트: " + event.getSubCategory()));
        };
    }

    private Mono<Void> handleReadyEvent(GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        return readyService.readyAndPrepare(roomId, player, true)
                .flatMap(started -> messageSender.sendMessageToAllUser(roomId,
                                WebSocketResDto.of(player, ResponseEvent.READY, "Ready 했습니다."))
                        .then(Mono.defer(() -> started ? handleAllReadyEvent(roomId) : Mono.empty())));
    }

    private Mono<Void> handleUnreadyEvent(GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        return readyService.readyAndPrepare(roomId, player, false)
                .flatMap(ignored -> messageSender.sendMessageToAllUser(roomId,
                        WebSocketResDto.of(player, ResponseEvent.UNREADY, "Ready 취소 했습니다.")));
    }

    private Mono<Void> handleAllReadyEvent(long roomId) {
        WebSocketResDto<Void> startDto = new WebSocketResDto<>(
                PLAYER_NOTHING,
                ResponseEvent.START,
                "게임이 시작됐습니다."
        );
        return messageSender.sendMessageToAllUser(roomId, startDto);
    }
}
