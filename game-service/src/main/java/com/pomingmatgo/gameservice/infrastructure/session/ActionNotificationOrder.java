package com.pomingmatgo.gameservice.infrastructure.session;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayDeque;
import java.util.List;

// 게임 락 안에서 예약한 액션 순서를 유지한다. 송신·대기 구독은 호출자가 소유한다.
public final class ActionNotificationOrder {
    private final ArrayDeque<Reservation> pending = new ArrayDeque<>();
    private boolean closed;

    public Reservation reserve() {
        Reservation reservation = new Reservation();
        boolean first;
        boolean rejected;
        synchronized (this) {
            rejected = closed;
            first = pending.isEmpty();
            if (!rejected) pending.addLast(reservation);
        }
        if (rejected) reservation.ready.tryEmitValue(false);
        else if (first) reservation.ready.tryEmitValue(true);
        return reservation;
    }

    public void close() {
        List<Reservation> removed;
        synchronized (this) {
            closed = true;
            removed = List.copyOf(pending);
            pending.clear();
        }
        removed.forEach(reservation -> reservation.ready.tryEmitValue(false));
    }

    public final class Reservation implements Disposable {
        private final Sinks.One<Boolean> ready = Sinks.one();

        public Mono<Boolean> ready() {
            return ready.asMono();
        }

        @Override
        public void dispose() {
            Reservation next = null;
            synchronized (ActionNotificationOrder.this) {
                boolean first = pending.peekFirst() == this;
                if (pending.remove(this) && first) next = pending.peekFirst();
            }
            // 대기 중 취소는 자신만 제거하며, 앞 액션을 건너뛰어 뒤 액션을 열지 않는다.
            ready.tryEmitValue(false);
            if (next != null) next.ready.tryEmitValue(true);
        }
    }
}
