package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/** 방 생성마다 새 identity를 부여하며, 종료된 수명의 타이머 조작을 거부한다. */
@Component
public class RoomTimerLifecycle {
    private final Map<Long, Object> rooms = new HashMap<>();

    public synchronized void open(long roomId) {
        rooms.put(roomId, new Object());
    }

    public synchronized void close(long roomId) {
        rooms.remove(roomId);
    }

    synchronized boolean isOpen(long roomId) {
        return rooms.containsKey(roomId);
    }

    synchronized void clear() {
        rooms.clear();
    }

    /** 송신이나 상태 변경을 시작하기 전에 호출하며, 이미 묶인 실행은 원래 수명을 유지한다. */
    public synchronized TurnScheduler bind(long roomId, TurnScheduler scheduler) {
        if (scheduler instanceof BoundScheduler bound) {
            if (bound.roomId != roomId) throw new IllegalArgumentException("Different room scheduler");
            return bound;
        }
        return new BoundScheduler(roomId, rooms.get(roomId), scheduler);
    }

    private final class BoundScheduler implements TurnScheduler {
        private final long roomId;
        private final Object identity;
        private final TurnScheduler delegate;

        private BoundScheduler(long roomId, Object identity, TurnScheduler delegate) {
            this.roomId = roomId;
            this.identity = identity;
            this.delegate = delegate;
        }

        private boolean current(long requestedRoomId) {
            return requestedRoomId == roomId && identity != null && rooms.get(roomId) == identity;
        }

        @Override
        public void scheduleAutoPlay(long roomId, int round, int turn, Player player, long deadline, GamePhase phase) {
            synchronized (RoomTimerLifecycle.this) {
                if (current(roomId)) delegate.scheduleAutoPlay(roomId, round, turn, player, deadline, phase);
            }
        }

        @Override
        public void cancelAutoPlay(long roomId) {
            synchronized (RoomTimerLifecycle.this) {
                if (current(roomId)) delegate.cancelAutoPlay(roomId);
            }
        }

        @Override
        public long getRemainingTurnMillis(long roomId) {
            return delegate.getRemainingTurnMillis(roomId);
        }
    }
}
