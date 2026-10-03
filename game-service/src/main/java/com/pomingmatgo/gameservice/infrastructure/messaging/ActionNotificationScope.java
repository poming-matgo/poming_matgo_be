package com.pomingmatgo.gameservice.infrastructure.messaging;

import com.pomingmatgo.gameservice.infrastructure.session.ActionNotificationOrder;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

// Mono.defer 안에서 구독마다 생성하고, 전체 처리의 doFinally에서 회수한다.
public final class ActionNotificationScope implements Disposable {
    private final long roomId;
    private final SessionManager sessions;
    private final Supplier<UnaryOperator<Context>> captureRecipients;
    private final AtomicReference<UnaryOperator<Context>> recipients = new AtomicReference<>();
    private final AtomicReference<ActionNotificationOrder.Reservation> order = new AtomicReference<>();
    private final Disposable.Swap owned = Disposables.swap();

    public ActionNotificationScope(long roomId, SessionManager sessions,
                                   Supplier<UnaryOperator<Context>> captureRecipients) {
        this.roomId = roomId;
        this.sessions = sessions;
        this.captureRecipients = captureRecipients;
    }

    /** 필수 처리 완료 콜백의 게임 락 안에서 한 번 호출한다. */
    public void capture() {
        recipients.set(captureRecipients.get());
        ActionNotificationOrder.Reservation reservation = sessions.reserveNotifications(roomId);
        order.set(reservation);
        // 수락 후 호출자가 먼저 취소돼도 늦게 생성된 예약을 즉시 회수한다.
        if (reservation != null) owned.update(reservation);
    }

    /** 락 밖에서 호출하며, 해당 액션의 안내 전체를 하나의 예약으로 보낸다. */
    public Mono<Void> send(Supplier<Mono<Void>> notification) {
        ActionNotificationOrder.Reservation reservation = order.get();
        Mono<Void> send = Mono.defer(notification).contextWrite(recipients.get());
        return reservation == null ? send : reservation.ready()
                .flatMap(allowed -> allowed ? send : Mono.empty());
    }

    @Override
    public void dispose() {
        owned.dispose();
    }
}
