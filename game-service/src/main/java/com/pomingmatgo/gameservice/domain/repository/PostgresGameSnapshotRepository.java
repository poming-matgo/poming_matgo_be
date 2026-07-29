package com.pomingmatgo.gameservice.domain.repository;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

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
        // 같은 (game_id, seq) 재도착은 무시 — 스냅샷은 write-once
        return generations.currentGeneration(snapshot.roomId())
                .flatMap(gameId -> gameLogDatabaseClient.sql("""
                                INSERT INTO game_snapshot (game_id, seq, room_id, state)
                                VALUES (:gameId, :seq, :roomId, CAST(:state AS jsonb))
                                ON CONFLICT DO NOTHING
                                """)
                        .bind("gameId", gameId)
                        .bind("seq", snapshot.seq())
                        .bind("roomId", snapshot.roomId())
                        .bind("state", toJson(snapshot))
                        .then());
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
