package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 게임 세대(gameId) 관리 — 같은 방의 게임 재시작 시 seq가 1부터라 (roomId, seq)만으론 유일하지 않다.
 * 캐시는 인스턴스 로컬(sticky routing 전제)이고 miss 시 DB 최신 세대 폴백이라 정합성엔 영향 없다
 */
@Component
@ConditionalOnProperty(name = "game.log.store", havingValue = "postgres")
@RequiredArgsConstructor
public class PostgresGameGenerations {

    private final DatabaseClient gameLogDatabaseClient;
    private final ConcurrentHashMap<Long, Long> current = new ConcurrentHashMap<>();

    /** DECK_INIT = 새 세대 시작 — 새 game_id를 발급하고 현재 세대로 캐시한다 */
    public Mono<Long> startNew(long roomId) {
        return gameLogDatabaseClient
                .sql("INSERT INTO game_generation (room_id) VALUES (:roomId) RETURNING game_id")
                .bind("roomId", roomId)
                .map(row -> row.get("game_id", Long.class))
                .one()
                .doOnNext(gameId -> current.put(roomId, gameId));
    }

    /** 방의 현재 세대. 캐시 miss(evict·재시작 후)면 DB 최신 세대 폴백, 세대가 없으면 empty */
    public Mono<Long> currentGeneration(long roomId) {
        Long cached = current.get(roomId);
        if (cached != null) {
            return Mono.just(cached);
        }
        return gameLogDatabaseClient
                .sql("SELECT max(game_id) AS game_id FROM game_generation WHERE room_id = :roomId")
                .bind("roomId", roomId)
                .map(row -> Optional.ofNullable(row.get("game_id", Long.class)))
                .one()
                .flatMap(Mono::justOrEmpty);
    }

    public void evict(long roomId) {
        current.remove(roomId);
    }

    // 방 정리 시 캐시 회수 — writer 실패로 markCompleted가 건너뛰어져도 새지 않는다 (CLAUDE.md 규칙 6)
    @EventListener
    public void onRoomCleanedUp(RoomCleanedUpEvent event) {
        evict(event.roomId());
    }
}
