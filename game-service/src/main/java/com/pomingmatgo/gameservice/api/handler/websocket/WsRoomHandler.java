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
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.util.context.Context;
import com.pomingmatgo.gameservice.infrastructure.session.ActionNotificationOrder;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

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
            AtomicReference<ActionNotificationOrder.Reservation> order = new AtomicReference<>();
            AtomicReference<UnaryOperator<Context>> recipients = new AtomicReference<>();
            Disposable.Swap owned = Disposables.swap();
            return readyService.readyAndPrepare(roomId, player, ready, () -> {
                        // 상태 완료 순서는 게임 락 안에서 예약하고 송신 대기는 락 밖에 둔다.
                        recipients.set(messageSender.captureRecipients(roomId));
                        ActionNotificationOrder.Reservation reservation = sessionManager.reserveNotifications(roomId);
                        order.set(reservation);
                        // 수락 후 취소가 먼저 도착해도 늦게 생성된 예약을 회수한다.
                        if (reservation != null) owned.update(reservation);
                    })
                    .flatMap(started -> {
                        Mono<Void> send = Mono.defer(() -> messageSender.sendMessageToAllUser(roomId,
                                        WebSocketResDto.of(player, ready ? ResponseEvent.READY : ResponseEvent.UNREADY,
                                                ready ? "Ready 했습니다." : "Ready 취소 했습니다.")))
                                .then(Mono.defer(() -> started ? handleAllReadyEvent(roomId) : Mono.empty()))
                                .contextWrite(recipients.get());
                        ActionNotificationOrder.Reservation reservation = order.get();
                        return reservation == null ? send : reservation.ready()
                                .flatMap(allowed -> allowed ? send : Mono.empty());
                    })
                    .doFinally(signal -> owned.dispose());
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
