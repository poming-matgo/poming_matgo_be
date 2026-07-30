package com.pomingmatgo.gameservice.domain.repository;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.lease.RoomLeaseManager;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

// 스냅샷 = 최신 세대의 seq 시점 상태 JSON. 세대 조회가 실패하면 버린다 — 유실은 replay 연장일 뿐 (인터페이스 계약).
// lease 활성 시 좀비의 스냅샷은 fencing 가드로 조용히 버려진다 — 상실 검출·통보는 로그 append 경로가 맡는다
@Component
@ConditionalOnProperty(name = "game.log.store", havingValue = "postgres")
public class PostgresGameSnapshotRepository implements GameSnapshotRepository {

    private final DatabaseClient gameLogDatabaseClient;
    private final PostgresGameGenerations generations;
    private final RoomLeaseManager leaseManager;
    private final ObjectMapper mapper;

    public PostgresGameSnapshotRepository(DatabaseClient gameLogDatabaseClient, PostgresGameGenerations generations,
                                          RoomLeaseManager leaseManager) {
        this.gameLogDatabaseClient = gameLogDatabaseClient;
        this.generations = generations;
        this.leaseManager = leaseManager;
        this.mapper = snapshotMapper();
    }

    // 웹 계층 ObjectMapper와 분리 — @JsonIgnore/getter 노출 정책과 무관하게 필드 기준으로 원상 복원해야 한다
    private static ObjectMapper snapshotMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE));
        return mapper;
    }

    @Override
    public Mono<Void> save(GameSnapshot snapshot) {
        return saveAll(List.of(snapshot));
    }

    // cross-room 배치 = insert 1왕복. 세대 조회는 대부분 캐시 hit이고, 세대 없는 방의 스냅샷은 버린다 (인터페이스 계약)
    @Override
    public Mono<Void> saveAll(List<GameSnapshot> batch) {
        boolean fenced = leaseManager.fencingEnabled();
        return Flux.fromIterable(batch)
                // lease 미보유 방의 스냅샷은 버린다 — 유실 허용 계약이라 통보 없음
                .filter(snapshot -> !fenced || leaseManager.tokenOf(snapshot.roomId()) != null)
                .concatMap(snapshot -> generations.currentGeneration(snapshot.roomId())
                        .map(gameId -> new Row(gameId, snapshot)))
                .collectList()
                .flatMap(rows -> insertRows(rows, fenced));
    }

    private record Row(long gameId, GameSnapshot snapshot) {}

    private Mono<Void> insertRows(List<Row> rows, boolean fenced) {
        if (rows.isEmpty()) {
            return Mono.empty();
        }
        StringBuilder sql = new StringBuilder(fenced
                ? "INSERT INTO game_snapshot (game_id, seq, room_id, state) "
                        + "SELECT v.game_id, v.seq, v.room_id, v.state FROM (VALUES "
                : "INSERT INTO game_snapshot (game_id, seq, room_id, state) VALUES ");
        for (int i = 0; i < rows.size(); i++) {
            sql.append(i > 0 ? ", " : "")
                    .append("(:gameId").append(i).append(", :seq").append(i)
                    .append(", :roomId").append(i).append(", CAST(:state").append(i).append(" AS jsonb)");
            if (fenced) {
                sql.append(", :fencingToken").append(i);
            }
            sql.append(')');
        }
        if (fenced) {
            sql.append(") AS v(game_id, seq, room_id, state, fencing_token) ")
                    .append("JOIN room_lease l ON l.room_id = v.room_id AND l.fencing_token = v.fencing_token");
        }
        // 같은 (game_id, seq) 재도착은 무시 — 스냅샷은 write-once
        sql.append(" ON CONFLICT DO NOTHING");
        DatabaseClient.GenericExecuteSpec spec = gameLogDatabaseClient.sql(sql.toString());
        for (int i = 0; i < rows.size(); i++) {
            GameSnapshot snapshot = rows.get(i).snapshot();
            spec = spec.bind("gameId" + i, rows.get(i).gameId())
                    .bind("seq" + i, snapshot.seq())
                    .bind("roomId" + i, snapshot.roomId())
                    .bind("state" + i, toJson(snapshot));
            if (fenced) {
                // filter 통과 후 상실로 토큰이 회수됐을 수 있다 — 낡은 값(-1)은 join에 걸리지 않아 그 방 행만 버려진다
                Long token = leaseManager.tokenOf(snapshot.roomId());
                spec = spec.bind("fencingToken" + i, token != null ? token : -1L);
            }
        }
        return spec.then();
    }

    @Override
    public Mono<GameSnapshot> findLatest(long roomId) {
        return gameLogDatabaseClient.sql("""
                        SELECT state FROM game_snapshot
                        WHERE room_id = :roomId
                          AND game_id = (SELECT max(game_id) FROM game_generation WHERE room_id = :roomId)
                        ORDER BY seq DESC
                        LIMIT 1
                        """)
                .bind("roomId", roomId)
                .map(row -> row.get("state", String.class))
                .one()
                .map(this::fromJson);
    }

    private String toJson(GameSnapshot snapshot) {
        try {
            return mapper.writeValueAsString(snapshot);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("스냅샷 직렬화 실패 — roomId=" + snapshot.roomId() + ", seq=" + snapshot.seq(), e);
        }
    }

    private GameSnapshot fromJson(String json) {
        try {
            return mapper.readValue(json, GameSnapshot.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("스냅샷 역직렬화 실패", e);
        }
    }
}
