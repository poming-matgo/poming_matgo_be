package com.pomingmatgo.gameservice.global.session;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SessionManager {
    private final ConcurrentHashMap<Long, RoomSessionData> sessions = new ConcurrentHashMap<>();
    //(roomId, RoomSessionData)

    public void addRoom(long roomId) {
        sessions.putIfAbsent(roomId, new RoomSessionData());
    }

    public void deleteRoom(long roomId) {
        sessions.remove(roomId);
    }

    public void addPlayer(long roomId, int playerNum, WebSocketSession session) {
        if(playerNum==1)
            sessions.get(roomId).setPlayer1Session(session);
        else
            sessions.get(roomId).setPlayer2Session(session);
    }

    public void deletePlayer(long roomId, int playerNum) {
        if (playerNum == 1)
            sessions.get(roomId).setPlayer1Session(null);
        else
            sessions.get(roomId).setPlayer2Session(null);
    }

    public WebSocketSession getSession(long roomId, int playerNum) {
        if(playerNum==1)
            return sessions.get(roomId).getPlayer1Session();
        else
            return sessions.get(roomId).getPlayer2Session();
    }

    public void removeRoom(Long roomId) {
        sessions.remove(roomId);
    }

    public Collection<WebSocketSession> getAllUser(long roomId) {
        Collection<WebSocketSession> userSessions = new ArrayList<>();
        RoomSessionData roomSessionData = sessions.get(roomId);

        if (roomSessionData.getPlayer1Session() != null) {
            userSessions.add(roomSessionData.getPlayer1Session());
        }
        if (roomSessionData.getPlayer2Session() != null) {
            userSessions.add(roomSessionData.getPlayer2Session());
        }

        return userSessions;
    }
}