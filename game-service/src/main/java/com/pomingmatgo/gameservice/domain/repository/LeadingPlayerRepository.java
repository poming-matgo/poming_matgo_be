package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.leadingplayer.ChooseLeadPlayer;
import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository

public class LeadingPlayerRepository {

    @Qualifier("cardRedisTemplate")
    @Autowired
    private ReactiveRedisOperations<String, String> cardredisOps;

    @Qualifier("chooseLeadPlayerTemplate")
    @Autowired
    private ReactiveRedisOperations<String, ChooseLeadPlayer> chooseLeadPlayerRedisOps;
    private static final String SELECTED_FIVE_CARD_KEY_PREFIX = "selectedFiveCards:";
    private static final String PLAYER_SELECTED_CARD_KEY_PREFIX = "playerSelectedCard:";

    public Mono<Void> saveSelectedCard(List<Card> cards, Long roomId) {
        String redisKey = SELECTED_FIVE_CARD_KEY_PREFIX + roomId;

        List<String> cardNames = cards.stream()
                .map(Enum::name)
                .toList();

        return cardredisOps.opsForList()
                .rightPushAll(redisKey, cardNames)
                .then();
    }

    public Mono<Card> getCardByIndex(Long roomId, int index) {
        String redisKey = SELECTED_FIVE_CARD_KEY_PREFIX + roomId;
        return cardredisOps.opsForList()
                .index(redisKey, index)
                .map(Card::valueOf);
    }

    public Flux<Card> getAllCards(Long roomId) {
        String redisKey = SELECTED_FIVE_CARD_KEY_PREFIX + roomId;
        return cardredisOps.opsForList()
                .range(redisKey, 0, -1)  // 전체 리스트 반환
                .map(Card::valueOf);
    }

    public Mono<Void> savePlayerSelectedCard(Long roomId, ChooseLeadPlayer chooseLeadPlayer) {
        String redisKey = PLAYER_SELECTED_CARD_KEY_PREFIX + roomId;
        return chooseLeadPlayerRedisOps.opsForValue().set(redisKey, chooseLeadPlayer)
                .then();
    }

    public Mono<ChooseLeadPlayer> getPlayerSelectedCard(Long roomId) {
        String redisKey = PLAYER_SELECTED_CARD_KEY_PREFIX + roomId;
        return chooseLeadPlayerRedisOps.opsForValue().get(redisKey)
                .defaultIfEmpty(new ChooseLeadPlayer());
    }
}
