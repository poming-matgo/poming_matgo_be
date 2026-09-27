package com.pomingmatgo.gameservice.infrastructure.session;

import com.pomingmatgo.gameservice.domain.Player;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.SignalType;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class SessionManager {
    private static final Duration REPLACED_SESSION_CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private final Disposable.Composite pendingCloses = Disposables.composite();
    private final ConcurrentHashMap<Long, RoomSessionData> roomSessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> sessionToRoomMap = new ConcurrentHashMap<>();

    public record PlayerContext(long roomId, long userId, int playerNum) {}

    public Mono<Void> addPlayer(long roomId, Player player, long userId, WebSocketSession session) {
        return Mono.fromRunnable(() -> {
            RoomSessionData roomData = roomSessions.computeIfAbsent(roomId, k -> new RoomSessionData());
            // 동시에 재접속해도 후속 요청이 이전 세션의 매핑을 제거할 수 있도록, 세션 교체 전에 매핑을 등록한다.
            sessionToRoomMap.put(session.getId(), roomId);
            WebSocketSession old = roomData.replacePlayer(player, userId, session);

            // 이전 세션의 disconnect가 새 연결에 영향을 주지 않도록, 매핑을 먼저 제거한 뒤 연결을 닫는다.
            if (old != null && !old.getId().equals(session.getId())) {
                sessionToRoomMap.remove(old.getId());
                closeReplacedSession(roomId, old);
            }
        });
    }

    private void closeReplacedSession(long roomId, WebSocketSession old) {
        BaseSubscriber<Void> close = new BaseSubscriber<>() {
            @Override
            protected void hookOnError(Throwable error) {
                log.warn("Replaced session [{}] close failed in room [{}]", old.getId(), roomId, error);
            }

            @Override
            protected void hookFinally(SignalType type) {
                pendingCloses.remove(this);
            }
        };
        // 즉시 완료되거나 서버 종료와 겹쳐도 구독을 정리할 수 있도록, 추적 목록에 먼저 등록한다.
        if (pendingCloses.add(close)) {
            Mono.defer(old::close).timeout(REPLACED_SESSION_CLOSE_TIMEOUT).subscribe(close);
        }
    }

    @PreDestroy
    public void shutdown() {
        pendingCloses.dispose();
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

    /** expected가 주어지면 현재 세션의 ID가 일치할 때만 제거하고, null이면 현재 세션을 제거한다. */
    public void deletePlayer(long roomId, int playerNum, WebSocketSession expected) {
        RoomSessionData data = roomSessions.get(roomId);
        if (data == null) return;

        WebSocketSession removed = data.removeIfCurrent(playerNum, expected);
        if (removed != null) {
            sessionToRoomMap.remove(removed.getId());
        }
    }

    public WebSocketSession getSession(long roomId, int playerNum) {
        // 조회 직전에 방이 제거됐을 수 있으므로, 방이 없으면 null을 반환한다.
        RoomSessionData data = roomSessions.get(roomId);
        if (data == null) return null;
        return data.getSession(playerNum);
    }

    public Mono<Void> removeRoom(Long roomId) {
        return Mono.fromRunnable(() -> {
            RoomSessionData removed = roomSessions.remove(roomId);
            if (removed == null) return;
            // 개별 disconnect 처리 없이 방이 삭제돼도 세션→방 매핑이 남지 않도록 함께 제거한다.
            removed.activeSessions().forEach(session -> sessionToRoomMap.remove(session.getId()));
        });
    }

    public Collection<WebSocketSession> getAllUser(long roomId) {
        RoomSessionData roomSessionData = roomSessions.get(roomId);
        // 방이 이미 제거됐다면 빈 목록을 반환해 해당 방으로 메시지를 보내지 않는다.
        if (roomSessionData == null) return List.of();

        return roomSessionData.activeSessions();
    }

}
