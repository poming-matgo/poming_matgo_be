package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.SYSTEM_ERROR;

@Repository
public class InstalledCardRepository {
    @Qualifier("cardRedisTemplate")
    @Autowired
    private ReactiveRedisOperations<String, String> redisOps;
    private static final String PLAYER1_CARD_KEY_PREFIX = "player1Card:";
    private static final String PLAYER2_CARD_KEY_PREFIX = "player2Card:";
    private static final String REVEALED_CARD_KEY_PREFIX = "revealedCard:";
    private static final String HIDDEN_CARD_KEY_PREFIX = "hiddenCard:";

    public Mono<Boolean> saveCards(List<Card> cards, long roomId, String keyPrefix) {
        String redisKey = keyPrefix + roomId;
        List<String> cardNames = cards.stream()
                .map(Enum::name)
                .toList();
        return redisOps.opsForList()
                .rightPushAll(redisKey, cardNames)
                .map(count -> count > 0);
    }


    public Mono<Boolean> deleteAllRevealedCardByMonth(long roomId, int month) {
        String redisKey = String.format("%s%d:%d", REVEALED_CARD_KEY_PREFIX, roomId, month);
        return redisOps.delete(redisKey)
                .thenReturn(true);
    }

    public Mono<Boolean> deleteRevealedCard(long roomId, Card card) {
        int month = card.getMonth();
        String redisKey = String.format("%s%d:%d",  REVEALED_CARD_KEY_PREFIX, roomId, month);
        return redisOps.opsForSet().remove(redisKey, card)
                .map(removedCount -> removedCount > 0);
    }

    public Mono<Boolean> savePlayerCards(List<Card> cards, long roomId, Player player) {
        String keyPrefix = getKeyPrefixForPlayer(player);
        return saveCards(cards, roomId, keyPrefix);
    }

    public Mono<Boolean> deletePlayerCards(long roomId, Player player) {
        String keyPrefix = getKeyPrefixForPlayer(player);
        return deleteAllPlayerCard(roomId, keyPrefix);
    }

    private Mono<Boolean> deleteAllPlayerCard(long roomId, String keyPrefix) {
        String redisKey = keyPrefix + roomId;
        return redisOps.delete(redisKey)
                .thenReturn(true);
    }

    public Mono<Boolean> saveRevealedCard(List<Card> cards, long roomId) {
        return Flux.fromIterable(cards)
                .collectMultimap(Card::getMonth, Enum::name)
                .flatMapMany(map -> Flux.fromIterable(map.entrySet()))
                .flatMap(entry -> {
                    int month = entry.getKey();
                    Collection<String> cardsInMonth = entry.getValue();
                    String redisKey = String.format("%s%d:%d", REVEALED_CARD_KEY_PREFIX, roomId, month);
                    return redisOps.opsForSet()
                            .add(redisKey, cardsInMonth.toArray(new String[0]))
                            .map(count -> count > 0);
                })
                .all(Boolean::booleanValue);
    }

    public Mono<Boolean> saveHiddenCard(List<Card> cards, long roomId) {
        return saveCards(cards, roomId, HIDDEN_CARD_KEY_PREFIX);
    }

    public Flux<Card> getRevealedCardByMonth(long roomId, long month) {
        String redisKey = String.format("%s%d:%d", REVEALED_CARD_KEY_PREFIX, roomId, month);
        return redisOps.opsForSet()
                .members(redisKey)
                .map(Card::valueOf);
    }

    public Mono<Card> getTopCard(long roomId) {
        String redisKey = HIDDEN_CARD_KEY_PREFIX + roomId;
        return redisOps.opsForList()
                .leftPop(redisKey)
                .map(Card::valueOf);
    }

    public Flux<Card> getCards(long roomId, String keyPrefix) {
        String redisKey = keyPrefix + roomId;
        return redisOps.opsForList()
                .range(redisKey, 0, -1)
                .map(Card::valueOf);
    }
    public Flux<Card> getPlayerCards(Long roomId, Player player) {
        String keyPrefix = getKeyPrefixForPlayer(player);
        return getCards(roomId, keyPrefix);
    }

    public Mono<Void> updatePlayerCards(long roomId, Player player, List<Card> cards) {
        return deletePlayerCards(roomId, player)
                .then(savePlayerCards(cards, roomId, player))
                .then();
    }

    private String getKeyPrefixForPlayer(Player player) {
        switch (player) {
            case PLAYER_1:
                return PLAYER1_CARD_KEY_PREFIX;
            case PLAYER_2:
                return PLAYER2_CARD_KEY_PREFIX;
            default:
                throw new WebSocketBusinessException(SYSTEM_ERROR);
        }
    }
}

