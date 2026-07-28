package com.pomingmatgo.gameservice.global.session;

import com.pomingmatgo.gameservice.domain.Player;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SessionManager {
    private final ConcurrentHashMap<Long, RoomSessionData> roomSessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> sessionToRoomMap = new ConcurrentHashMap<>();

    public record PlayerContext(long roomId, long userId, int playerNum) {}

    public Mono<Void> addPlayer(long roomId, Player player, long userId, WebSocketSession session) {
        return Mono.fromRunnable(() -> {
            RoomSessionData roomData = roomSessions.computeIfAbsent(roomId, k -> new RoomSessionData());
            // 매핑 등록이 슬롯 교체보다 먼저다 — 동시 접속에서 밀린 쪽의 매핑을 이긴 쪽이 반드시 보고 지운다
            sessionToRoomMap.put(session.getId(), roomId);
            WebSocketSession old = roomData.replacePlayer(player, userId, session);

            // 교체된 낡은 세션은 매핑을 먼저 지우고 kick — 그래야 뒤늦은 disconnect 처리가
            // getPlayerContext에서 비어 no-op이 되고 새 세션을 지우지 못한다
            if (old != null && !old.getId().equals(session.getId())) {
                sessionToRoomMap.remove(old.getId());
                old.close().subscribe();
            }
        });
    }

    public Mono<PlayerContext> getPlayerContext(WebSocketSession session) {
        return Mono.fromCallable(() -> {
            Long roomId = sessionToRoomMap.get(session.getId());
            if (roomId == null) return null;
            RoomSessionData roomData = roomSessions.get(roomId);
            if (roomData == null)  return null;

            RoomSessionData.Occupant occupant = roomData.occupantOf(session);
            if (occupant == null) return null;

            return new PlayerContext(roomId, occupant.userId(), occupant.playerNum());
        });
    }

    public Mono<Void> addRoom(long roomId) {
        return Mono.fromRunnable(() ->
                roomSessions.putIfAbsent(roomId, new RoomSessionData())
        );
    }

    /** identity guard — 무조건 비우면 그 사이 재접속한 새 세션을 지워버린다 */
    public void deletePlayer(long roomId, int playerNum, WebSocketSession expected) {
        RoomSessionData data = roomSessions.get(roomId);
        if (data == null) return;

        WebSocketSession removed = data.removeIfCurrent(playerNum, expected);
        if (removed != null) {
            sessionToRoomMap.remove(removed.getId());
        }
    }

    public WebSocketSession getSession(long roomId, int playerNum) {
        // removeRoom과 동시 호출될 수 있어 null 방어
        RoomSessionData data = roomSessions.get(roomId);
        if (data == null) return null;
        return data.getSession(playerNum);
    }

    public Mono<Void> removeRoom(Long roomId) {
        return Mono.fromRunnable(() -> {
            RoomSessionData removed = roomSessions.remove(roomId);
            if (removed == null) return;
            // deletePlayer가 놓친 잔여 세션의 매핑까지 정리 (누수 방지)
            removed.activeSessions().forEach(session -> sessionToRoomMap.remove(session.getId()));
        });
    }

    public Collection<WebSocketSession> getAllUser(long roomId) {
        RoomSessionData roomSessionData = roomSessions.get(roomId);
        // removeRoom과 동시 호출될 수 있어 null 방어 — 빈 목록이면 브로드캐스트가 no-op
        if (roomSessionData == null) return List.of();

        return roomSessionData.activeSessions();
    }

}