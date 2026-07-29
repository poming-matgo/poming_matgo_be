package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import io.r2dbc.spi.Readable;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// 실물 durable 저장소 — 배치 1건 = multi-row insert 1회 왕복. 복구 조회는 방의 최신 세대만 대상이다
@Component
@ConditionalOnProperty(name = "game.log.store", havingValue = "postgres")
@RequiredArgsConstructor
public class PostgresGameLogRepository implements GameLogRepository {

    private final DatabaseClient gameLogDatabaseClient;
    private final PostgresGameGenerations generations;

    @Override
    public Mono<Void> append(long roomId, List<GameLogRecord> batch) {
        if (batch.isEmpty()) {
            return Mono.empty();
        }
        // DECK_INIT은 세대의 첫 레코드라 배치 선두로만 도착한다 (방 단위 ordered writer가 순서 보장)
        return resolveGameId(batch).flatMap(id ->
                insertRows(batch.stream().map(record -> new Row(id, record)).toList()));
    }

    // cross-room 배치 = insert 1왕복. 같은 방의 세그먼트 간 세대 해소 순서(현 세대 조회 → startNew)를 지켜야 하므로 순차 resolve
    @Override
    public Mono<Void> appendAll(List<GameLogRecord> batch) {
        if (batch.isEmpty()) {
            return Mono.empty();
        }
        return Flux.fromIterable(GameLogRepository.segmentByGeneration(batch))
                .concatMap(segment -> resolveGameId(segment)
                        .map(id -> segment.stream().map(record -> new Row(id, record)).toList()))
                .collectList()
                .flatMap(rowGroups -> insertRows(rowGroups.stream().flatMap(List::stream).toList()));
    }

    private Mono<Long> resolveGameId(List<GameLogRecord> segment) {
        long roomId = segment.get(0).roomId();
        return segment.get(0).type() == GameCommandType.DECK_INIT
                ? generations.startNew(roomId)
                : generations.currentGeneration(roomId)
                        .switchIfEmpty(Mono.error(() -> new IllegalStateException("게임 세대 없음 — roomId=" + roomId)));
    }

    private record Row(long gameId, GameLogRecord record) {}

    private Mono<Void> insertRows(List<Row> rows) {
        StringBuilder sql = new StringBuilder(
                "INSERT INTO game_log (game_id, seq, room_id, type, player, card_index, go, deck, prev_phase, next_phase) VALUES ");
        for (int i = 0; i < rows.size(); i++) {
            sql.append(i > 0 ? ", " : "")
                    .append("(:gameId").append(i).append(", :seq").append(i).append(", :roomId").append(i)
                    .append(", :type").append(i).append(", :player").append(i).append(", :cardIndex").append(i)
                    .append(", :go").append(i).append(", :deck").append(i)
                    .append(", :prevPhase").append(i).append(", :nextPhase").append(i).append(')');
        }
        DatabaseClient.GenericExecuteSpec spec = gameLogDatabaseClient.sql(sql.toString());
        for (int i = 0; i < rows.size(); i++) {
            GameLogRecord r = rows.get(i).record();
            spec = spec.bind("gameId" + i, rows.get(i).gameId())
                    .bind("roomId" + i, r.roomId())
                    .bind("seq" + i, r.seq())
                    .bind("type" + i, r.type().name())
                    .bind("cardIndex" + i, r.cardIndex())
                    .bind("go" + i, r.go());
            spec = bindText(spec, "player" + i, r.player() == null ? null : r.player().name());
            spec = bindText(spec, "deck" + i, serializeDeck(r.deck()));
            spec = bindText(spec, "prevPhase" + i, r.prevPhase() == null ? null : r.prevPhase().name());
            spec = bindText(spec, "nextPhase" + i, r.nextPhase() == null ? null : r.nextPhase().name());
        }
        return spec.then();
    }

    private DatabaseClient.GenericExecuteSpec bindText(DatabaseClient.GenericExecuteSpec spec, String name, String value) {
        return value != null ? spec.bind(name, value) : spec.bindNull(name, String.class);
    }

    private String serializeDeck(List<Card> deck) {
        return deck == null ? null : deck.stream().map(Enum::name).collect(Collectors.joining(","));
    }

    @Override
    public Flux<GameLogRecord> findAllFromSeq(long roomId, long fromSeq) {
        return gameLogDatabaseClient.sql("""
                        SELECT seq, type, player, card_index, go, deck, prev_phase, next_phase
                        FROM game_log
                        WHERE room_id = :roomId
                          AND game_id = (SELECT max(game_id) FROM game_generation WHERE room_id = :roomId)
                          AND seq >= :fromSeq
                        ORDER BY seq
                        """)
                .bind("roomId", roomId)
                .bind("fromSeq", fromSeq)
                .map(row -> toRecord(roomId, row))
                .all();
    }

    private GameLogRecord toRecord(long roomId, Readable row) {
        String player = row.get("player", String.class);
        String deck = row.get("deck", String.class);
        String prevPhase = row.get("prev_phase", String.class);
        String nextPhase = row.get("next_phase", String.class);
        return new GameLogRecord(
                roomId,
                row.get("seq", Long.class),
                GameCommandType.valueOf(row.get("type", String.class)),
                player == null ? null : Player.valueOf(player),
                row.get("card_index", Integer.class),
                Boolean.TRUE.equals(row.get("go", Boolean.class)),
                deck == null ? null : Arrays.stream(deck.split(",")).map(Card::valueOf).toList(),
                prevPhase == null ? null : GamePhase.valueOf(prevPhase),
                nextPhase == null ? null : GamePhase.valueOf(nextPhase));
    }

    @Override
    public Mono<Void> markCompleted(long roomId) {
        return generations.currentGeneration(roomId)
                .flatMap(gameId -> gameLogDatabaseClient
                        .sql("UPDATE game_generation SET completed = TRUE WHERE game_id = :gameId")
                        .bind("gameId", gameId)
                        .then())
                // 완료된 세대 캐시는 즉시 회수 — 이후 늦은 조회(비동기 스냅샷 등)는 DB 최신 세대 폴백으로 안전하다
                .doFinally(signal -> generations.evict(roomId));
    }
}
