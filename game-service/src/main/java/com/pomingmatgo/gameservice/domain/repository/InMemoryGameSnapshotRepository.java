package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

// 개발·테스트용 durability 스탠드인. seq 키 정렬 보관이라 저장이 늦게 도착해도 findLatest는 최고 seq를 반환한다.
// 한계: 같은 방의 새 게임은 seq가 1부터라 이전 세대 스냅샷과 구분 불가 — 게임 세대 식별자(gameId)는 영속 스키마에서 도입
@Component
@ConditionalOnProperty(name = "game.log.store", havingValue = "in-memory")
public class InMemoryGameSnapshotRepository implements GameSnapshotRepository {

    private final ConcurrentHashMap<Long, ConcurrentSkipListMap<Long, GameSnapshot>> snapshots = new ConcurrentHashMap<>();

    @Override
    public Mono<Void> save(GameSnapshot snapshot) {
        return Mono.fromRunnable(() ->
                snapshots.computeIfAbsent(snapshot.roomId(), k -> new ConcurrentSkipListMap<>())
                        .put(snapshot.seq(), snapshot));
    }

    @Override
    public Mono<GameSnapshot> findLatest(long roomId) {
        return Mono.fromCallable(() -> {
            ConcurrentSkipListMap<Long, GameSnapshot> roomSnapshots = snapshots.get(roomId);
            Map.Entry<Long, GameSnapshot> latest = roomSnapshots != null ? roomSnapshots.lastEntry() : null;
            return latest != null ? latest.getValue() : null;
        });
    }

    public int count(long roomId) {
        ConcurrentSkipListMap<Long, GameSnapshot> roomSnapshots = snapshots.get(roomId);
        return roomSnapshots != null ? roomSnapshots.size() : 0;
    }
}
