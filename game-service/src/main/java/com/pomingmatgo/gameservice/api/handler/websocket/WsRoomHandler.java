package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.application.room.RoomReadyService;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.messaging.ResponseEvent;
import com.pomingmatgo.gameservice.infrastructure.messaging.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import com.pomingmatgo.gameservice.infrastructure.messaging.ActionNotificationScope;
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
    private final SessionManager sessionManager;


    public Mono<Void> handleRoomEvent(RequestEvent<?> event, GameState gameState, Player player) {
        return switch (event.getSubCategory()) {
            case READY -> handleReadyEvent(gameState, player, true);
            case UNREADY -> handleReadyEvent(gameState, player, false);
            default -> Mono.error(new IllegalStateException("처리기가 없는 ROOM 이벤트: " + event.getSubCategory()));
        };
    }

    private Mono<Void> handleReadyEvent(GameState gameState, Player player, boolean ready) {
        long roomId = gameState.getRoomId();
        return Mono.defer(() -> {
            ActionNotificationScope notifications = new ActionNotificationScope(roomId, sessionManager,
                    () -> messageSender.captureRecipients(roomId));
            return readyService.readyAndPrepare(roomId, player, ready, notifications::capture)
                    .flatMap(started -> notifications.send(() -> messageSender.sendMessageToAllUser(roomId,
                                    WebSocketResDto.of(player, ready ? ResponseEvent.READY : ResponseEvent.UNREADY,
                                            ready ? "Ready 했습니다." : "Ready 취소 했습니다."))
                            .then(Mono.defer(() -> started ? handleAllReadyEvent(roomId) : Mono.empty()))))
                    .doFinally(signal -> notifications.dispose());
        });
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
