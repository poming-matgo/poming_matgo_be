package com.pomingmatgo.gameservice.domain.repository;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

// 스냅샷 = 최신 세대의 seq 시점 상태 JSON. 세대 조회가 실패하면 버린다 — 유실은 replay 연장일 뿐 (인터페이스 계약)
@Component
@ConditionalOnProperty(name = "game.log.store", havingValue = "postgres")
public class PostgresGameSnapshotRepository implements GameSnapshotRepository {

    private final DatabaseClient gameLogDatabaseClient;
    private final PostgresGameGenerations generations;
    private final ObjectMapper mapper;

    public PostgresGameSnapshotRepository(DatabaseClient gameLogDatabaseClient, PostgresGameGenerations generations) {
        this.gameLogDatabaseClient = gameLogDatabaseClient;
        this.generations = generations;
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
        return Flux.fromIterable(batch)
                .concatMap(snapshot -> generations.currentGeneration(snapshot.roomId())
                        .map(gameId -> new Row(gameId, snapshot)))
                .collectList()
                .flatMap(this::insertRows);
    }

    private record Row(long gameId, GameSnapshot snapshot) {}

    private Mono<Void> insertRows(List<Row> rows) {
        if (rows.isEmpty()) {
            return Mono.empty();
        }
        StringBuilder sql = new StringBuilder("INSERT INTO game_snapshot (game_id, seq, room_id, state) VALUES ");
        for (int i = 0; i < rows.size(); i++) {
            sql.append(i > 0 ? ", " : "")
                    .append("(:gameId").append(i).append(", :seq").append(i)
                    .append(", :roomId").append(i).append(", CAST(:state").append(i).append(" AS jsonb))");
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
