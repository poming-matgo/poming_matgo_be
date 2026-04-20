package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository
public class RedisAcquiredCardRepository implements AcquiredCardRepository {

    private static final String KEY_FORMAT = "game:%d:player:%d:hand";

    private final ReactiveRedisOperations<String, String> redisOps;

    public RedisAcquiredCardRepository(@Qualifier("acquiredCardRedisTemplate") ReactiveRedisOperations<String, String> redisOps) {
        this.redisOps = redisOps;
    }

    private String generateKey(long roomId, long playerId) {
        return String.format(KEY_FORMAT, roomId, playerId);
    }

    public Mono<Long> addCards(long roomId, long playerId, List<Card> cards) {
        if (cards == null || cards.isEmpty()) {
            return Mono.just(0L);
        }

        String[] cardNames = cards.stream().map(Enum::name).toArray(String[]::new);
        return redisOps.opsForSet().add(generateKey(roomId, playerId), cardNames);
    }

    public Mono<List<Card>> getAllCards(long roomId, long playerId) {
        return redisOps.opsForSet()
                .members(generateKey(roomId, playerId))
                .map(Card::valueOf)
                .collectList();
    }

    public Mono<Long> removeCard(long roomId, long playerId, Card card) {
        return redisOps.opsForSet().remove(generateKey(roomId, playerId), card.name());
    }
}
