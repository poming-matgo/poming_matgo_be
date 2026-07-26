package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// 방 단위 쓰기는 @GameLock으로 직렬화되므로 computeIfAbsent 이후의 셋 뮤테이션은 단일 스레드다
@Profile("in-memory")
@Repository
public class InMemoryAcquiredCardRepository implements AcquiredCardRepository {

    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Card>>> store = new ConcurrentHashMap<>();

    @Override
    public Mono<Long> addCards(long roomId, long playerId, List<Card> cards) {
        if (cards == null || cards.isEmpty()) {
            return Mono.just(0L);
        }
        return Mono.fromCallable(() -> {
            store.computeIfAbsent(roomId, k -> new ConcurrentHashMap<>())
                 .computeIfAbsent(playerId, k -> ConcurrentHashMap.newKeySet())
                 .addAll(cards);
            return (long) cards.size();
        });
    }

    @Override
    public Mono<List<Card>> getAllCards(long roomId, long playerId) {
        return Mono.fromCallable(() -> {
            ConcurrentHashMap<Long, Set<Card>> room = store.get(roomId);
            return room != null
                    ? new ArrayList<>(room.getOrDefault(playerId, Collections.emptySet()))
                    : Collections.emptyList();
        });
    }

    @Override
    public Mono<Long> removeCard(long roomId, long playerId, Card card) {
        return Mono.fromCallable(() -> {
            ConcurrentHashMap<Long, Set<Card>> room = store.get(roomId);
            Set<Card> set = room != null ? room.get(playerId) : null;
            return (set != null && set.remove(card)) ? 1L : 0L;
        });
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> store.remove(roomId));
    }
}
