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
    //(roomId, RoomSessionData)
    private final ConcurrentHashMap<String, Long> sessionToRoomMap = new ConcurrentHashMap<>();

    public record PlayerContext(long roomId, long userId, int playerNum) {}

    public Mono<Void> addPlayer(long roomId, Player player, long userId, WebSocketSession session) {
        return Mono.fromRunnable(() -> {
            RoomSessionData roomData = roomSessions.computeIfAbsent(roomId, k -> new RoomSessionData());
            roomData.addPlayer(player, userId, session);
            sessionToRoomMap.put(session.getId(), roomId);
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

    public void deletePlayer(long roomId, int playerNum) {
        RoomSessionData data = roomSessions.get(roomId);
        if (data == null) return;

        WebSocketSession sessionToRemove = (playerNum == 1) ? data.getPlayer1Session() : data.getPlayer2Session();
        if (sessionToRemove != null) {
            sessionToRoomMap.remove(sessionToRemove.getId());
        }

        if (playerNum == 1) data.setPlayer1Session(null);
        else data.setPlayer2Session(null);
    }

    public WebSocketSession getSession(long roomId, int playerNum) {
        // 방 제거(removeRoom)와 동시 호출될 수 있으므로 null 방어
        RoomSessionData data = roomSessions.get(roomId);
        if (data == null) return null;
        return (playerNum == 1) ? data.getPlayer1Session() : data.getPlayer2Session();
    }

    public Mono<Void> removeRoom(Long roomId) {
        return Mono.fromRunnable(() -> {
            RoomSessionData removed = roomSessions.remove(roomId);
            if (removed == null) return;
            // deletePlayer로 정리 안 된 잔여 session이 있다면 sessionToRoomMap도 함께 정리 (cleanup safety)
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
        // 방 제거(removeRoom)와 동시 호출될 수 있으므로 null 방어 — 빈 목록 반환으로 브로드캐스트 no-op
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