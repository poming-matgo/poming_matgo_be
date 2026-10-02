package com.pomingmatgo.gameservice.application.pregame;

import com.pomingmatgo.gameservice.application.game.TurnFlowService;
import com.pomingmatgo.gameservice.application.pregame.PreGameStartService.PreparedStart;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.infrastructure.messaging.GameMessageSender;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.util.context.Context;
import com.pomingmatgo.gameservice.infrastructure.session.ActionNotificationOrder;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_NOTHING;
import static com.pomingmatgo.gameservice.domain.TurnTiming.TURN_TIMEOUT_MILLIS;

@Service
@RequiredArgsConstructor
public class PreGameFlowService {
    private final PreGameStartService startService;
    private final GameMessageSender gameMessageSender;
    private final TurnFlowService turnFlowService;
    private final SessionManager sessionManager;

    public Mono<Void> processLeaderSelection(GameState gameState, Player player, int cardIndex) {
        long roomId = gameState.getRoomId();
        return Mono.defer(() -> {
            AtomicReference<UnaryOperator<Context>> recipients = new AtomicReference<>();
            AtomicReference<ActionNotificationOrder.Reservation> order = new AtomicReference<>();
            Disposable.Swap owned = Disposables.swap();
            return startService.selectAndPrepare(roomId, player, cardIndex, () -> {
                        // 선택 저장 또는 시작 필수 처리 완료 순서를 락 안에서 예약한다.
                        recipients.set(gameMessageSender.captureRecipients(roomId));
                        ActionNotificationOrder.Reservation reservation = sessionManager.reserveNotifications(roomId);
                        order.set(reservation);
                        // 수락 후 먼저 취소된 호출의 늦은 예약도 회수한다.
                        if (reservation != null) owned.update(reservation);
                    })
                    .map(Optional::of)
                    .defaultIfEmpty(Optional.empty())
                    .flatMap(start -> {
                        Mono<Void> send = Mono.defer(() -> gameMessageSender.sendLeaderSelectionMessage(roomId, player, cardIndex))
                                .then(Mono.defer(() -> start.isPresent() ? notifyGameStart(roomId, start.get()) : Mono.empty()))
                                .contextWrite(recipients.get());
                        ActionNotificationOrder.Reservation reservation = order.get();
                        return reservation == null ? send : reservation.ready()
                                .flatMap(allowed -> allowed ? send : Mono.empty());
                    })
                    .doFinally(signal -> owned.dispose());
        });
    }

    private Mono<Void> notifyGameStart(long roomId, PreparedStart start) {
        return Mono.defer(() -> gameMessageSender.sendLeaderSelectionResult(roomId, start.result()))
                .then(Mono.defer(() -> gameMessageSender.sendDistributedCardInfo(roomId, start.cards())))
                .then(Mono.defer(() -> start.draw()
                        ? turnFlowService.announceGameOver(start.state(), PLAYER_NOTHING)
                        : gameMessageSender.sendTurnInfo(start.state(), TURN_TIMEOUT_MILLIS)));
    }

}
