package com.pomingmatgo.gameservice.global.session;

import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
    private final ConcurrentHashMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    public boolean addSessionIfAbsent(String userId, WebSocketSession session) {
        return sessions.putIfAbsent(userId, session) == null;
        //todo: 기존 연결을 끊고 새 연결로 대체하는 경우에대한 코드 추가해야함
        //java
        //코드 복사
    }

    public WebSocketSession getSession(String userId) {
        return sessions.get(userId);
    }

    public void removeSession(String userId) {
        sessions.remove(userId);
    }

    public Collection<WebSocketSession> getAllSessions() {
        return sessions.values();
    }
}