package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.ChooseLeadPlayer;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Profile("in-memory")
@Repository
public class InMemoryLeadingPlayerRepository implements LeadingPlayerRepository {

    private static final Object PRESENT = new Object();

    private final ConcurrentHashMap<Long, List<Card>> selectedCards = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> player1Month = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> player2Month = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Object> trigger = new ConcurrentHashMap<>();

    @Override
    public Mono<Void> saveSelectedCard(List<Card> cards, Long roomId) {
        return Mono.fromRunnable(() -> {
            // @GameLock 직렬화 보장 → computeIfAbsent 후 리스트 뮤테이션은 단일 스레드
            selectedCards.computeIfAbsent(roomId, k -> new ArrayList<>()).addAll(cards);
        });
    }

    @Override
    public Mono<Card> getCardByIndex(Long roomId, int index) {
        return Mono.fromCallable(() -> {
            List<Card> cards = selectedCards.get(roomId);
            return cards != null ? cards.get(index) : null;
        });
    }

    @Override
    public Mono<List<Card>> getAllCards(Long roomId) {
        return Mono.fromCallable(() ->
                new ArrayList<>(selectedCards.getOrDefault(roomId, Collections.emptyList()))
        );
    }

    @Override
    public Mono<Void> savePlayerMonth(Long roomId, Player player, int month) {
        return Mono.fromRunnable(() -> {
            if (player == Player.PLAYER_1) {
                player1Month.put(roomId, month);
            } else {
                player2Month.put(roomId, month);
            }
        });
    }

    @Override
    public Mono<ChooseLeadPlayer> getPlayerSelectedCard(Long roomId) {
        return Mono.fromCallable(() -> new ChooseLeadPlayer(
                player1Month.getOrDefault(roomId, 0),
                player2Month.getOrDefault(roomId, 0)
        ));
    }

    @Override
    public Mono<Boolean> tryClaimLeaderSelectionTrigger(Long roomId) {
        return Mono.fromCallable(() -> trigger.putIfAbsent(roomId, PRESENT) == null);
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> {
            selectedCards.remove(roomId);
            player1Month.remove(roomId);
            player2Month.remove(roomId);
            trigger.remove(roomId);
        });
    }
}
