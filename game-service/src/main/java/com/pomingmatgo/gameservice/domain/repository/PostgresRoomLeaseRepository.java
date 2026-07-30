package com.pomingmatgo.gameservice.domain.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

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

    @Override
    public Mono<Void> recordDeadlines(List<RoomDeadline> batch) {
        if (batch.isEmpty()) {
            return Mono.empty();
        }
        // conflate 배치 = UPDATE 1왕복. token join이 fencing 가드 — 인수당한 방의 행만 조용히 빠진다
        StringBuilder sql = new StringBuilder("""
                UPDATE room_lease l SET turn_deadline_epoch_millis = v.deadline
                FROM (VALUES """);
        for (int i = 0; i < batch.size(); i++) {
            sql.append(i > 0 ? ", " : "")
                    .append("(CAST(:roomId").append(i).append(" AS BIGINT), CAST(:token").append(i)
                    .append(" AS BIGINT), CAST(:deadline").append(i).append(" AS BIGINT))");
        }
        sql.append(") AS v(room_id, fencing_token, deadline) ")
                .append("WHERE l.room_id = v.room_id AND l.fencing_token = v.fencing_token");
        DatabaseClient.GenericExecuteSpec spec = gameLogDatabaseClient.sql(sql.toString());
        for (int i = 0; i < batch.size(); i++) {
            RoomDeadline d = batch.get(i);
            spec = spec.bind("roomId" + i, d.roomId())
                    .bind("token" + i, d.fencingToken())
                    .bind("deadline" + i, d.deadlineEpochMillis());
        }
        return spec.then();
    }

    @Override
    public Flux<Long> findExpiredRoomIds() {
        return gameLogDatabaseClient.sql("""
                        SELECT room_id FROM room_lease
                        WHERE expires_at < now() AND owner_instance <> :releasedOwner
                        """)
                .bind("releasedOwner", RELEASED_OWNER)
                .map(row -> row.get("room_id", Long.class))
                .all();
    }

    @Override
    public Mono<Takeover> takeover(long roomId, String newOwner, Duration duration) {
        // WHERE의 만료 조건이 곧 상호 배제 — 동시 인수 시도 중 UPDATE 승자 1명만 행을 얻는다
        return gameLogDatabaseClient.sql("""
                        UPDATE room_lease
                        SET owner_instance = :owner,
                            fencing_token = fencing_token + 1,
                            expires_at = now() + make_interval(secs => :durationSeconds)
                        WHERE room_id = :roomId AND expires_at < now() AND owner_instance <> :releasedOwner
                        RETURNING fencing_token, turn_deadline_epoch_millis
                        """)
                .bind("owner", newOwner)
                .bind("durationSeconds", seconds(duration))
                .bind("roomId", roomId)
                .bind("releasedOwner", RELEASED_OWNER)
                .map(row -> new Takeover(row.get("fencing_token", Long.class),
                        row.get("turn_deadline_epoch_millis", Long.class)))
                .one();
    }

    @Override
    public Mono<Void> abandon(long roomId, long fencingToken) {
        // owner를 released로 바꾸지 않는다 — 인수 스캔이 다시 주울 수 있어야 한다 (복구 재시도)
        return gameLogDatabaseClient.sql("""
                        UPDATE room_lease SET expires_at = now()
                        WHERE room_id = :roomId AND fencing_token = :fencingToken
                        """)
                .bind("roomId", roomId)
                .bind("fencingToken", fencingToken)
                .then();
    }

    private double seconds(Duration duration) {
        return duration.toMillis() / 1000.0;
    }
}
