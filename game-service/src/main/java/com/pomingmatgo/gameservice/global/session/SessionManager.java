package com.pomingmatgo.gameservice.global.session;

import com.pomingmatgo.gameservice.domain.Player;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SessionManager {
    private final ConcurrentHashMap<Long, RoomSessionData> roomSessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> sessionToRoomMap = new ConcurrentHashMap<>();

    public record PlayerContext(long roomId, long userId, int playerNum) {}

    public Mono<Void> addPlayer(long roomId, Player player, long userId, WebSocketSession session) {
        return Mono.fromRunnable(() -> {
            RoomSessionData roomData = roomSessions.computeIfAbsent(roomId, k -> new RoomSessionData());
            WebSocketSession old = (player == Player.PLAYER_1)
                    ? roomData.getPlayer1Session() : roomData.getPlayer2Session();
            roomData.addPlayer(player, userId, session);
            sessionToRoomMap.put(session.getId(), roomId);

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

            long userId = -1;
            int playerNum = -1;
            if (session.equals(roomData.getPlayer1Session())) {
                userId = roomData.getPlayer1Id();
                playerNum = 1;
            } else if (session.equals(roomData.getPlayer2Session())) {
                userId = roomData.getPlayer2Id();
                playerNum = 2;
            }

            if (userId == -1) {
                return null;
            }

            return new PlayerContext(roomId, userId, playerNum);
        }).subscribeOn(Schedulers.boundedElastic());
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

        WebSocketSession current = (playerNum == 1) ? data.getPlayer1Session() : data.getPlayer2Session();
        if (current == null) return;
        if (expected != null && !current.getId().equals(expected.getId())) return;

        sessionToRoomMap.remove(current.getId());
        if (playerNum == 1) data.setPlayer1Session(null);
        else data.setPlayer2Session(null);
    }

    public WebSocketSession getSession(long roomId, int playerNum) {
        // removeRoom과 동시 호출될 수 있어 null 방어
        RoomSessionData data = roomSessions.get(roomId);
        if (data == null) return null;
        return (playerNum == 1) ? data.getPlayer1Session() : data.getPlayer2Session();
    }

    public Mono<Void> removeRoom(Long roomId) {
        return Mono.fromRunnable(() -> {
            RoomSessionData removed = roomSessions.remove(roomId);
            if (removed == null) return;
            // deletePlayer가 놓친 잔여 세션의 매핑까지 정리 (누수 방지)
            if (removed.getPlayer1Session() != null) {
                sessionToRoomMap.remove(removed.getPlayer1Session().getId());
            }
            if (removed.getPlayer2Session() != null) {
                sessionToRoomMap.remove(removed.getPlayer2Session().getId());
            }
        });
    }

    public Collection<WebSocketSession> getAllUser(long roomId) {
        Collection<WebSocketSession> userSessions = new ArrayList<>();
        RoomSessionData roomSessionData = roomSessions.get(roomId);
        // removeRoom과 동시 호출될 수 있어 null 방어 — 빈 목록이면 브로드캐스트가 no-op
        if (roomSessionData == null) return userSessions;

        if (roomSessionData.getPlayer1Session() != null) {
            userSessions.add(roomSessionData.getPlayer1Session());
        }
        if (roomSessionData.getPlayer2Session() != null) {
            userSessions.add(roomSessionData.getPlayer2Session());
        }

        return userSessions;
    }

}