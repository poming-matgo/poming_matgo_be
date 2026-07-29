package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// 방 단위 쓰기는 @GameLock으로 직렬화되므로 computeIfAbsent 이후의 컬렉션 뮤테이션은 단일 스레드다
@Profile("in-memory")
@Repository
public class InMemoryInstalledCardRepository implements InstalledCardRepository {

    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, List<Card>>> playerCards = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Integer, Set<Card>>> revealedCards = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ArrayDeque<Card>> hiddenDeck = new ConcurrentHashMap<>();

    @Override
    public Mono<Boolean> savePlayerCards(List<Card> cards, long roomId, Player player) {
        return Mono.fromCallable(() -> {
            playerCards.computeIfAbsent(roomId, k -> new ConcurrentHashMap<>())
                       .computeIfAbsent(player.name(), k -> new ArrayList<>())
                       .addAll(cards);
            return true;
        });
    }

    @Override
    public Mono<Boolean> deletePlayerCards(long roomId, Player player) {
        return Mono.fromCallable(() -> {
            ConcurrentHashMap<String, List<Card>> room = playerCards.get(roomId);
            if (room != null) room.remove(player.name());
            return true;
        });
    }

    @Override
    public Mono<Boolean> saveRevealedCard(List<Card> cards, long roomId) {
        return Mono.fromCallable(() -> {
            ConcurrentHashMap<Integer, Set<Card>> room = revealedCards.computeIfAbsent(roomId, k -> new ConcurrentHashMap<>());
            for (Card card : cards) {
                room.computeIfAbsent(card.getMonth(), k -> new HashSet<>()).add(card);
            }
            return true;
        });
    }

    @Override
    public Mono<Boolean> saveHiddenCard(List<Card> cards, long roomId) {
        return Mono.fromCallable(() -> {
            hiddenDeck.computeIfAbsent(roomId, k -> new ArrayDeque<>()).addAll(cards);
            return true;
        });
    }

    @Override
    public Mono<Boolean> deleteAllRevealedCardByMonth(long roomId, int month) {
        return Mono.fromCallable(() -> {
            ConcurrentHashMap<Integer, Set<Card>> room = revealedCards.get(roomId);
            if (room != null) room.remove(month);
            return true;
        });
    }

    @Override
    public Mono<Boolean> deleteRevealedCard(long roomId, Card card) {
        return Mono.fromCallable(() -> {
            ConcurrentHashMap<Integer, Set<Card>> room = revealedCards.get(roomId);
            Set<Card> set = room != null ? room.get(card.getMonth()) : null;
            return set != null && set.remove(card);
        });
    }

    // HashSet 순회 순서는 JVM 실행마다 다르다(enum identity hash) — natural order 정렬로 고정 (인터페이스 계약)
    @Override
    public Mono<List<Card>> getRevealedCardByMonth(long roomId, int month) {
        return Mono.fromCallable(() -> {
            ConcurrentHashMap<Integer, Set<Card>> room = revealedCards.get(roomId);
            if (room == null) return Collections.emptyList();
            List<Card> cards = new ArrayList<>(room.getOrDefault(month, Collections.emptySet()));
            Collections.sort(cards);
            return cards;
        });
    }

    @Override
    public Mono<List<Card>> getAllRevealedCards(long roomId) {
        return Mono.fromCallable(() -> {
            ConcurrentHashMap<Integer, Set<Card>> room = revealedCards.get(roomId);
            if (room == null) return Collections.emptyList();
            List<Card> all = new ArrayList<>();
            room.values().forEach(all::addAll);
            Collections.sort(all);
            return all;
        });
    }

    @Override
    public Mono<Card> drawTopCard(long roomId) {
        return Mono.fromCallable(() -> {
            ArrayDeque<Card> deck = hiddenDeck.get(roomId);
            return deck != null ? deck.poll() : null;
        });
    }

    @Override
    public Mono<List<Card>> getHiddenCards(long roomId) {
        return Mono.fromCallable(() -> {
            ArrayDeque<Card> deck = hiddenDeck.get(roomId);
            return deck != null ? new ArrayList<>(deck) : Collections.emptyList();
        });
    }

    @Override
    public Mono<List<Card>> getPlayerCards(long roomId, Player player) {
        return Mono.fromCallable(() -> {
            ConcurrentHashMap<String, List<Card>> room = playerCards.get(roomId);
            return room != null
                    ? new ArrayList<>(room.getOrDefault(player.name(), Collections.emptyList()))
                    : Collections.emptyList();
        });
    }

    @Override
    public Mono<Void> updatePlayerCards(long roomId, Player player, List<Card> cards) {
        return deletePlayerCards(roomId, player)
                .then(savePlayerCards(cards, roomId, player))
                .then();
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> {
            playerCards.remove(roomId);
            revealedCards.remove(roomId);
            hiddenDeck.remove(roomId);
        });
    }
}
