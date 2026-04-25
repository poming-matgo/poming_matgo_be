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

import org.springframework.context.annotation.Profile;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.SYSTEM_ERROR;

@Profile("redis")
@Repository
public class RedisInstalledCardRepository implements InstalledCardRepository {
    @Qualifier("cardRedisTemplate")
    @Autowired
    private ReactiveRedisOperations<String, String> redisOps;
    private static final String PLAYER1_CARD_KEY_FORMAT = "game:%d:cards:player1Card:";
    private static final String PLAYER2_CARD_KEY_FORMAT = "game:%d:cards:player2Card:";
    private static final String REVEALED_CARD_KEY_FORMAT = "game:%d:cards:revealed:%d";
    private static final String HIDDEN_CARD_KEY_FORMAT = "game:%d:cards:hidden";

    private String getKeyPrefixForPlayer(Player player, long roomId) {
        return switch (player) {
            case PLAYER_1 -> String.format(PLAYER1_CARD_KEY_FORMAT, roomId);
            case PLAYER_2 -> String.format(PLAYER2_CARD_KEY_FORMAT, roomId);
            default -> throw new WebSocketBusinessException(SYSTEM_ERROR);
        };
    }

    private String generateRevealedCardKey(long roomId, long month) {
        return String.format(REVEALED_CARD_KEY_FORMAT, roomId, month);
    }

    private String generateHiddenCardKey(long roomId) {
        return String.format(HIDDEN_CARD_KEY_FORMAT, roomId);
    }


    public Mono<Boolean> saveCards(List<Card> cards, String redisKey) {
        if (cards == null || cards.isEmpty()) {
            return Mono.just(true);
        }
        List<String> cardNames = cards.stream()
                .map(Enum::name)
                .toList();
        return redisOps.opsForList()
                .rightPushAll(redisKey, cardNames)
                .map(count -> count > 0);
    }


    public Mono<Boolean> deleteAllRevealedCardByMonth(long roomId, int month) {
        String redisKey = generateRevealedCardKey(roomId, month);
        return redisOps.delete(redisKey)
                .thenReturn(true);
    }

    public Mono<Boolean> deleteRevealedCard(long roomId, Card card) {
        int month = card.getMonth();
        String redisKey = generateRevealedCardKey(roomId, month);
        return redisOps.opsForSet().remove(redisKey, card.name())
                .map(removedCount -> removedCount > 0);
    }

    public Mono<Boolean> savePlayerCards(List<Card> cards, long roomId, Player player) {
        String redisKey = getKeyPrefixForPlayer(player, roomId);
        return saveCards(cards, redisKey);
    }

    public Mono<Boolean> deletePlayerCards(long roomId, Player player) {
        return deleteAllPlayerCard(getKeyPrefixForPlayer(player, roomId));
    }

    private Mono<Boolean> deleteAllPlayerCard(String redisKey) {
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
                    String redisKey = generateRevealedCardKey(roomId, month);
                    return redisOps.opsForSet()
                            .add(redisKey, cardsInMonth.toArray(new String[0]))
                            .map(count -> count > 0);
                })
                .all(Boolean::booleanValue);
    }

    public Mono<Boolean> saveHiddenCard(List<Card> cards, long roomId) {
        return saveCards(cards, generateHiddenCardKey(roomId));
    }

    public Mono<List<Card>> getRevealedCardByMonth(long roomId, long month) {
        String redisKey = generateRevealedCardKey(roomId, month);
        return redisOps.opsForSet()
                .members(redisKey)
                .map(Card::valueOf)
                .collectList();
    }

    public Mono<Card> getTopCard(long roomId) {
        String redisKey = generateHiddenCardKey(roomId);
        return redisOps.opsForList()
                .leftPop(redisKey)
                .map(Card::valueOf);
    }

    private Mono<List<Card>> getCards(String redisKey) {
        return redisOps.opsForList()
                .range(redisKey, 0, -1)
                .map(Card::valueOf)
                .collectList();
    }

    public Mono<List<Card>> getPlayerCards(Long roomId, Player player) {
        return getCards(getKeyPrefixForPlayer(player, roomId));
    }

    public Mono<Void> updatePlayerCards(long roomId, Player player, List<Card> cards) {
        return deletePlayerCards(roomId, player)
                .then(savePlayerCards(cards, roomId, player))
                .then();
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        String[] keys = new String[15];
        keys[0] = String.format(PLAYER1_CARD_KEY_FORMAT, roomId);
        keys[1] = String.format(PLAYER2_CARD_KEY_FORMAT, roomId);
        keys[2] = generateHiddenCardKey(roomId);
        for (int i = 0; i < 12; i++) {
            keys[3 + i] = generateRevealedCardKey(roomId, i + 1);
        }
        return redisOps.delete(keys).then();
    }
}
