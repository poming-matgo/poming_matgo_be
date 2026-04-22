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

@Profile("in-memory")
@Repository
public class InMemoryAcquiredCardRepository implements AcquiredCardRepository {

    private final ConcurrentHashMap<String, Set<Card>> store = new ConcurrentHashMap<>();

    private String key(long roomId, long playerId) {
        return roomId + ":" + playerId;
    }

    @Override
    public Mono<Long> addCards(long roomId, long playerId, List<Card> cards) {
        if (cards == null || cards.isEmpty()) {
            return Mono.just(0L);
        }
        return Mono.fromCallable(() -> {
            store.computeIfAbsent(key(roomId, playerId), k -> ConcurrentHashMap.newKeySet())
                 .addAll(cards);
            return (long) cards.size();
        });
    }

    @Override
    public Mono<List<Card>> getAllCards(long roomId, long playerId) {
        return Mono.fromCallable(() ->
                new ArrayList<>(store.getOrDefault(key(roomId, playerId), Collections.emptySet()))
        );
    }

    @Override
    public Mono<Long> removeCard(long roomId, long playerId, Card card) {
        return Mono.fromCallable(() -> {
            Set<Card> set = store.get(key(roomId, playerId));
            return (set != null && set.remove(card)) ? 1L : 0L;
        });
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() ->
                store.keySet().removeIf(k -> k.startsWith(roomId + ":"))
        );
    }
}
