package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
@Repository
public class ScoreCardRepository {
    @Qualifier("cardRedisTemplate")
    @Autowired
    private ReactiveRedisOperations<String, String> redisOps;
    private static final String PI_KEY_PREFIX = "pi:";
    private static final String TTI_KEY_PREFIX = "tti:";
    private static final String GWANG_KEY_PREFIX = "gwang:";
    private static final String KKUT_KEY_PREFIX = "kkut:";

    public Flux<Card> getCards(long roomId, long playerNum, String keyPrefix) {
        String redisKey = String.format("%s%d:%d", keyPrefix, roomId, playerNum);
        return redisOps.opsForList()
                .range(redisKey, 0, -1)
                .map(Card::valueOf);
    }
    public Flux<Card> getPiCards(Long roomId, Long playerNum) {
        return getCards(roomId, playerNum, PI_KEY_PREFIX);
    }

    public Flux<Card> getKkutCards(Long roomId, Long playerNum) {
        return getCards(roomId, playerNum, KKUT_KEY_PREFIX);
    }

    public Flux<Card> getGwangCards(Long roomId, Long playerNum) {
        return getCards(roomId, playerNum, GWANG_KEY_PREFIX);
    }

    public Flux<Card> getTtiCards(Long roomId, Long playerNum) {
        return getCards(roomId, playerNum, TTI_KEY_PREFIX);
    }

}
