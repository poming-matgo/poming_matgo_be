package com.pomingmatgo.gameservice.domain.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

// lease 판정 시계는 전부 DB now() — 인스턴스 wall clock을 쓰면 노드 간 시계 오차가 배타성 구멍이 된다.
// game.log.store=postgres 전제 (같은 DB의 gameLogDatabaseClient 재사용)
@Component
@ConditionalOnProperty(name = "game.lease.store", havingValue = "postgres")
@RequiredArgsConstructor
public class PostgresRoomLeaseRepository implements RoomLeaseRepository {

    // release는 만료 처리만 하고 owner를 sentinel로 바꾼다 — heartbeat(owner 일치)가 해제된 lease를 되살리지 못하게
    private static final String RELEASED_OWNER = "released";

    private final DatabaseClient gameLogDatabaseClient;

    @Override
    public Mono<Long> acquire(long roomId, String owner, Duration duration) {
        // 인수는 만료 lease에 대해서만 성립하고 그때마다 token이 오른다. 같은 인스턴스의 재획득(같은 방 새 게임)도 token을 올린다
        return gameLogDatabaseClient.sql("""
                        INSERT INTO room_lease (room_id, owner_instance, fencing_token, expires_at)
                        VALUES (:roomId, :owner, 1, now() + make_interval(secs => :durationSeconds))
                        ON CONFLICT (room_id) DO UPDATE SET
                            owner_instance = EXCLUDED.owner_instance,
                            fencing_token = room_lease.fencing_token + 1,
                            expires_at = EXCLUDED.expires_at
                        WHERE room_lease.expires_at < now() OR room_lease.owner_instance = EXCLUDED.owner_instance
                        RETURNING fencing_token
                        """)
                .bind("roomId", roomId)
                .bind("owner", owner)
                .bind("durationSeconds", seconds(duration))
                .map(row -> row.get("fencing_token", Long.class))
                .one();
    }

    @Override
    public Mono<Long> heartbeat(String owner, Duration duration) {
        // 만료된 lease는 연장하지 않는다 — 인수 스캔과 되살리기가 경합하면 배타성이 깨진다
        return gameLogDatabaseClient.sql("""
                        UPDATE room_lease
                        SET expires_at = now() + make_interval(secs => :durationSeconds)
                        WHERE owner_instance = :owner AND expires_at > now()
                        """)
                .bind("owner", owner)
                .bind("durationSeconds", seconds(duration))
                .fetch().rowsUpdated();
    }

    @Override
    public Mono<Long> currentToken(long roomId) {
        return gameLogDatabaseClient.sql("SELECT fencing_token FROM room_lease WHERE room_id = :roomId")
                .bind("roomId", roomId)
                .map(row -> row.get("fencing_token", Long.class))
                .one();
    }

    @Override
    public Mono<Void> release(long roomId, long fencingToken) {
        return gameLogDatabaseClient.sql("""
                        UPDATE room_lease
                        SET expires_at = now(), owner_instance = :releasedOwner
                        WHERE room_id = :roomId AND fencing_token = :fencingToken
                        """)
                .bind("releasedOwner", RELEASED_OWNER)
                .bind("roomId", roomId)
                .bind("fencingToken", fencingToken)
                .then();
    }

    private double seconds(Duration duration) {
        return duration.toMillis() / 1000.0;
    }
}
