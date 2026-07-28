package com.pomingmatgo.gameservice.global.session;

import com.pomingmatgo.gameservice.domain.Player;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.ArrayList;
import java.util.List;

// 슬롯은 event loop(읽기)와 boundedElastic(쓰기)이 함께 만지므로 모든 접근을 이 monitor로 직렬화한다.
// 비교와 교체가 갈라지면 재접속이 넣은 새 세션을 뒤늦은 disconnect가 지운다
public class RoomSessionData {
    private Long player1Id;
    private WebSocketSession player1Session;
    private Long player2Id;
    private WebSocketSession player2Session;

    public record Occupant(int playerNum, long userId) {}

    public synchronized WebSocketSession replacePlayer(Player player, long userId, WebSocketSession session) {
        if (player == Player.PLAYER_1) {
            WebSocketSession old = player1Session;
            player1Id = userId;
            player1Session = session;
            return old;
        }
        WebSocketSession old = player2Session;
        player2Id = userId;
        player2Session = session;
        return old;
    }

    public synchronized WebSocketSession removeIfCurrent(int playerNum, WebSocketSession expected) {
        WebSocketSession current = getSession(playerNum);
        if (current == null) return null;
        if (expected != null && !current.getId().equals(expected.getId())) return null;

        if (playerNum == 1) player1Session = null;
        else player2Session = null;
        return current;
    }

    public synchronized Occupant occupantOf(WebSocketSession session) {
        if (session.equals(player1Session)) return new Occupant(1, player1Id);
        if (session.equals(player2Session)) return new Occupant(2, player2Id);
        return null;
    }

    public synchronized WebSocketSession getSession(int playerNum) {
        return (playerNum == 1) ? player1Session : player2Session;
    }

    public synchronized List<WebSocketSession> activeSessions() {
        List<WebSocketSession> sessions = new ArrayList<>(2);
        if (player1Session != null) sessions.add(player1Session);
        if (player2Session != null) sessions.add(player2Session);
        return sessions;
    }
}
