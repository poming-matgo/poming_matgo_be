package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import com.pomingmatgo.gameservice.domain.lease.RoomLeaseManager;
import io.r2dbc.spi.Readable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// 실물 durable 저장소 — 배치 1건 = multi-row insert 1회 왕복. 복구 조회는 방의 최신 세대만 대상이다.
// lease 활성 시 모든 쓰기는 fencing token 가드를 통과해야 한다 — 좀비 노드의 낡은 쓰기를 DB가 거부한다 (2-B)
@Component
@ConditionalOnProperty(name = "game.log.store", havingValue = "postgres")
@RequiredArgsConstructor
@Slf4j
public class PostgresGameLogRepository implements GameLogRepository {

    private final DatabaseClient gameLogDatabaseClient;
    private final PostgresGameGenerations generations;
    private final RoomLeaseManager leaseManager;

    @Override
    public Mono<Void> append(long roomId, List<GameLogRecord> batch) {
        return appendAll(batch);
    }

    // cross-room 배치 = insert 1왕복. 같은 방의 세그먼트 간 세대 해소 순서(현 세대 조회 → startNew)를 지켜야 하므로 순차 resolve
    @Override
    public Mono<Void> appendAll(List<GameLogRecord> batch) {
        if (batch.isEmpty()) {
            return Mono.empty();
        }
        List<List<GameLogRecord>> segments = GameLogRepository.segmentByGeneration(batch);
        if (!leaseManager.fencingEnabled()) {
            return resolveRows(segments, null).flatMap(rows -> insertRows(rows, null).then());
        }

        Map<Long, Long> roomTokens = new HashMap<>();
        List<List<GameLogRecord>> owned = new ArrayList<>();
        for (List<GameLogRecord> segment : segments) {
            long roomId = segment.get(0).roomId();
            Long token = leaseManager.tokenOf(roomId);
            if (token == null) {
                log.warn("lease 미보유 방의 로그 배치 폐기 — roomId={}, {}건", roomId, segment.size());
            } else {
                roomTokens.put(roomId, token);
                owned.add(segment);
            }
        }
        if (owned.isEmpty()) {
            return Mono.empty();
        }
        // 기대보다 적게 들어갔다면(세대 발급 거부 포함) 그 배치의 방들 중 소유권을 잃은 방이 있다 — 검증 후 상실 통보
        int expected = owned.stream().mapToInt(List::size).sum();
        return resolveRows(owned, roomTokens)
                .flatMap(rows -> insertRows(rows, roomTokens)
                        .flatMap(inserted -> inserted < expected
                                ? leaseManager.verifyOwnership(roomTokens.keySet())
                                : Mono.empty()));
    }

    private Mono<List<Row>> resolveRows(List<List<GameLogRecord>> segments, Map<Long, Long> roomTokens) {
        return Flux.fromIterable(segments)
                .concatMap(segment -> resolveGameId(segment, roomTokens)
                        .map(id -> segment.stream().map(record -> new Row(id, record)).toList()))
                .collectList()
                .map(groups -> groups.stream().flatMap(List::stream).toList());
    }

    // DECK_INIT은 세대의 첫 레코드라 세그먼트 선두로만 도착한다 (방 단위 ordered writer가 순서 보장).
    // fencing 활성 시 세대 발급도 가드된다 — 거부되면 empty로 세그먼트째 폐기된다
    private Mono<Long> resolveGameId(List<GameLogRecord> segment, Map<Long, Long> roomTokens) {
        long roomId = segment.get(0).roomId();
        if (segment.get(0).type() == GameCommandType.DECK_INIT) {
            return roomTokens == null
                    ? generations.startNew(roomId)
                    : generations.startNew(roomId, roomTokens.get(roomId));
        }
        return generations.currentGeneration(roomId)
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("게임 세대 없음 — roomId=" + roomId)));
    }

    private record Row(long gameId, GameLogRecord record) {}

    private Mono<Long> insertRows(List<Row> rows, Map<Long, Long> roomTokens) {
        if (rows.isEmpty()) {
            return Mono.just(0L);
        }
        boolean fenced = roomTokens != null;
        StringBuilder sql = new StringBuilder(fenced
                ? "INSERT INTO game_log (game_id, seq, room_id, type, player, card_index, go, deck, prev_phase, next_phase) "
                        + "SELECT v.game_id, v.seq, v.room_id, v.type, v.player, v.card_index, v.go, v.deck, v.prev_phase, v.next_phase "
                        + "FROM (VALUES "
                : "INSERT INTO game_log (game_id, seq, room_id, type, player, card_index, go, deck, prev_phase, next_phase) VALUES ");
        for (int i = 0; i < rows.size(); i++) {
            sql.append(i > 0 ? ", " : "")
                    .append("(:gameId").append(i).append(", :seq").append(i).append(", :roomId").append(i)
                    .append(", :type").append(i).append(", :player").append(i).append(", :cardIndex").append(i)
                    .append(", :go").append(i).append(", :deck").append(i)
                    .append(", :prevPhase").append(i).append(", :nextPhase").append(i);
            if (fenced) {
                sql.append(", :fencingToken").append(i);
            }
            sql.append(')');
        }
        if (fenced) {
            // 좀비의 낡은 토큰은 join에 걸리지 않아 그 방의 행만 통째로 빠진다 — 거부 판정은 rowsUpdated로
            sql.append(") AS v(game_id, seq, room_id, type, player, card_index, go, deck, prev_phase, next_phase, fencing_token) ")
                    .append("JOIN room_lease l ON l.room_id = v.room_id AND l.fencing_token = v.fencing_token");
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
            if (fenced) {
                spec = spec.bind("fencingToken" + i, roomTokens.get(r.roomId()));
            }
        }
        return spec.fetch().rowsUpdated();
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
        // 완료 표시도 소유자의 쓰기다 — 소유권을 잃은 좀비가 인수자의 진행 중 세대를 완료로 오염시키면 복구가 막힌다.
        // 토큰 읽기는 defer로 — 조립 시점에 읽으면 release와의 순서 보장이 조립 순서에 좌우된다
        return Mono.defer(() -> {
            Long token = leaseManager.fencingEnabled() ? leaseManager.tokenOf(roomId) : null;
            if (leaseManager.fencingEnabled() && token == null) {
                return Mono.empty();
            }
            return generations.currentGeneration(roomId)
                    .flatMap(gameId -> markCompletedSql(roomId, gameId, token))
                    // 완료된 세대 캐시는 즉시 회수 — 이후 늦은 조회(비동기 스냅샷 등)는 DB 최신 세대 폴백으로 안전하다
                    .doFinally(signal -> generations.evict(roomId));
        });
    }

    private Mono<Void> markCompletedSql(long roomId, long gameId, Long fencingToken) {
        if (fencingToken == null) {
            return gameLogDatabaseClient
                    .sql("UPDATE game_generation SET completed = TRUE WHERE game_id = :gameId")
                    .bind("gameId", gameId)
                    .then();
        }
        return gameLogDatabaseClient.sql("""
                        UPDATE game_generation SET completed = TRUE
                        WHERE game_id = :gameId
                          AND EXISTS (SELECT 1 FROM room_lease WHERE room_id = :roomId AND fencing_token = :fencingToken)
                        """)
                .bind("gameId", gameId)
                .bind("roomId", roomId)
                .bind("fencingToken", fencingToken)
                .then();
    }
}
