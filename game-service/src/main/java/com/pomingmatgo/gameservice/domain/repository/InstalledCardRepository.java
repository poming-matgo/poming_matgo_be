package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository
public class InstalledCardRepository {
    @Qualifier("installedCardTemplate")
    @Autowired
    private ReactiveRedisOperations<String, List<String>> redisOps;
    private static final String PLAYER1_CARD_KEY_PREFIX = "player1Card:";
    private static final String PLAYER2_CARD_KEY_PREFIX = "player2Card:";
    private static final String REVEALED_CARD_KEY_PREFIX = "revealedCard:";
    private static final String HIDDEN_CARD_KEY_PREFIX = "hiddenCard:";
    public Mono<Boolean> saveCards(List<Card> cards, long roomId, String keyPrefix) {
        String redisKey = keyPrefix + roomId;
        List<String> cardNames = cards.stream()
                .map(Enum::name)
                .toList();
        return redisOps.opsForValue().set(redisKey, cardNames);
    }

    public Mono<Boolean> savePlayer1Card(List<Card> cards, long roomId) {
        return saveCards(cards, roomId, PLAYER1_CARD_KEY_PREFIX);
    }

    public Mono<Boolean> savePlayer2Card(List<Card> cards, long roomId) {
        return saveCards(cards, roomId, PLAYER2_CARD_KEY_PREFIX);
    }

    public Mono<Boolean> saveRevealedCard(List<Card> cards, long roomId) {
        return saveCards(cards, roomId, REVEALED_CARD_KEY_PREFIX);
    }

    public Mono<Boolean> saveHiddenCard(List<Card> cards, long roomId) {
        return saveCards(cards, roomId, HIDDEN_CARD_KEY_PREFIX);
    }
}
