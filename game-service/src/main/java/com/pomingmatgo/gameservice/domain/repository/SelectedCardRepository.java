package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository

public class SelectedCardRepository {

    @Qualifier("cardRedisTemplate")
    @Autowired
    private ReactiveRedisOperations<String, Card> redisOps;
    private static final String SELECTED_CARD_KEY_PREFIX = "selectedCards";

    public Mono<Void> saveSelectedCard(List<Card> cards, Long roomId) {
        String redisKey = SELECTED_CARD_KEY_PREFIX + roomId;
        return redisOps.opsForList()
                .rightPushAll(redisKey, cards)
                .then();
    }
}
