package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Profile("in-memory")
@Repository
public class InMemoryInstalledCardRepository implements InstalledCardRepository {

    private final ConcurrentHashMap<String, List<Card>> playerCards = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Card>> revealedCards = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ArrayDeque<Card>> hiddenDeck = new ConcurrentHashMap<>();

    private String playerKey(long roomId, Player player) {
        return roomId + ":" + player.name();
    }

    private String revealedKey(long roomId, int month) {
        return roomId + ":" + month;
    }

    @Override
    public Mono<Boolean> savePlayerCards(List<Card> cards, long roomId, Player player) {
        return Mono.fromCallable(() -> {
            playerCards.computeIfAbsent(playerKey(roomId, player), k -> new ArrayList<>())
                       .addAll(cards);
            return true;
        });
    }

    @Override
    public Mono<Boolean> deletePlayerCards(long roomId, Player player) {
        return Mono.fromCallable(() -> {
            playerCards.remove(playerKey(roomId, player));
            return true;
        });
    }

    @Override
    public Mono<Boolean> saveRevealedCard(List<Card> cards, long roomId) {
        return Mono.fromCallable(() -> {
            for (Card card : cards) {
                revealedCards.computeIfAbsent(revealedKey(roomId, card.getMonth()), k -> new HashSet<>())
                             .add(card);
            }
            return true;
        });
    }

    @Override
    public Mono<Boolean> saveHiddenCard(List<Card> cards, long roomId) {
        return Mono.fromCallable(() -> {
            hiddenDeck.computeIfAbsent(roomId, k -> new ArrayDeque<>())
                      .addAll(cards);
            return true;
        });
    }

    @Override
    public Mono<Boolean> deleteAllRevealedCardByMonth(long roomId, int month) {
        return Mono.fromCallable(() -> {
            revealedCards.remove(revealedKey(roomId, month));
            return true;
        });
    }

    @Override
    public Mono<Boolean> deleteRevealedCard(long roomId, Card card) {
        return Mono.fromCallable(() -> {
            Set<Card> set = revealedCards.get(revealedKey(roomId, card.getMonth()));
            return set != null && set.remove(card);
        });
    }

    @Override
    public Mono<List<Card>> getRevealedCardByMonth(long roomId, long month) {
        return Mono.fromCallable(() ->
                new ArrayList<>(revealedCards.getOrDefault(revealedKey(roomId, (int) month), Collections.emptySet()))
        );
    }

    @Override
    public Mono<Card> getTopCard(long roomId) {
        return Mono.fromCallable(() -> {
            ArrayDeque<Card> deck = hiddenDeck.get(roomId);
            return deck != null ? deck.poll() : null;
        });
    }

    @Override
    public Mono<List<Card>> getPlayerCards(Long roomId, Player player) {
        return Mono.fromCallable(() ->
                new ArrayList<>(playerCards.getOrDefault(playerKey(roomId, player), Collections.emptyList()))
        );
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
            playerCards.keySet().removeIf(k -> k.startsWith(roomId + ":"));
            revealedCards.keySet().removeIf(k -> k.startsWith(roomId + ":"));
            hiddenDeck.remove(roomId);
        });
    }
}
