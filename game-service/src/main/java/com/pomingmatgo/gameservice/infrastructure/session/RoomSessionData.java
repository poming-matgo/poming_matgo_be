package com.pomingmatgo.gameservice.infrastructure.session;

import com.pomingmatgo.gameservice.domain.Player;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.ArrayList;
import java.util.List;

// 슬롯의 비교·교체는 같은 monitor로 보호하고 대기자 통지는 monitor 밖에서 수행한다.
public class RoomSessionData {
    private Registration player1;
    private Registration player2;

    public record Occupant(int playerNum, long userId) {}
    public record Registration(long userId, WebSocketSession session, SnapshotDelivery snapshot) {}

    public WebSocketSession replacePlayer(Player player, long userId, WebSocketSession session,
                                          SnapshotDelivery snapshot) {
        Registration old;
        synchronized (this) {
            Registration next = new Registration(userId, session, snapshot);
            if (player == Player.PLAYER_1) {
                old = player1;
                player1 = next;
            } else {
                old = player2;
                player2 = next;
            }
        }
        if (old != null) old.snapshot().complete(false);
        return old == null ? null : old.session();
    }

    public WebSocketSession removeIfCurrent(int playerNum, WebSocketSession expected) {
        Registration removed;
        synchronized (this) {
            removed = playerNum == 1 ? player1 : player2;
            if (removed == null) return null;
            if (expected != null && !removed.session().getId().equals(expected.getId())) return null;
            if (playerNum == 1) player1 = null;
            else player2 = null;
        }
        removed.snapshot().complete(false);
        return removed.session();
    }

    public synchronized Occupant occupantOf(WebSocketSession session) {
        if (player1 != null && session.equals(player1.session())) return new Occupant(1, player1.userId());
        if (player2 != null && session.equals(player2.session())) return new Occupant(2, player2.userId());
        return null;
    }

    public synchronized WebSocketSession getSession(int playerNum) {
        Registration current = playerNum == 1 ? player1 : player2;
        return current == null ? null : current.session();
    }

    public synchronized List<Registration> registrations() {
        List<Registration> registrations = new ArrayList<>(2);
        if (player1 != null) registrations.add(player1);
        if (player2 != null) registrations.add(player2);
        return registrations;
    }

    public synchronized boolean isCurrent(Registration registration) {
        return player1 == registration || player2 == registration;
    }

    public List<WebSocketSession> activeSessions() {
        return registrations().stream().map(Registration::session).toList();
    }
}
