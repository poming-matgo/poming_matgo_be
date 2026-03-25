package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.ChooseLeadPlayer;
import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository

public class LeadingPlayerRepository {

    @Qualifier("cardRedisTemplate")
    @Autowired
    private ReactiveRedisOperations<String, String> cardRedisOps;
    //<String, List<String>>을 <String, String>으로 변경

    @Qualifier("chooseLeadPlayerTemplate")
    @Autowired
    private ReactiveRedisOperations<String, ChooseLeadPlayer> chooseLeadPlayerRedisOps;
    private static final String SELECTED_FIVE_CARD_KEY_FORMAT = "game:%d:lead:select5";
    private static final String PLAYER_SELECTED_CARD_KEY_FORMAT = "game:%d:lead:playerChoice";


    private String generateSelectedFiveCardKey(long roomId) {
        return String.format(SELECTED_FIVE_CARD_KEY_FORMAT, roomId);
    }

    private String generatePlayerSelectedCardKey(long roomId) {
        return String.format(PLAYER_SELECTED_CARD_KEY_FORMAT, roomId);
    }

    public Mono<Void> saveSelectedCard(List<Card> cards, Long roomId) {
        String redisKey = generateSelectedFiveCardKey(roomId);

        List<String> cardNames = cards.stream()
                .map(Enum::name)
                .toList();

        return cardRedisOps.opsForList()
                .rightPushAll(redisKey, cardNames)
                .then();
    }

    public Mono<Card> getCardByIndex(Long roomId, int index) {
        String redisKey = generateSelectedFiveCardKey(roomId);
        return cardRedisOps.opsForList()
                .index(redisKey, index)
                .map(Card::valueOf);
    }

    public Mono<List<Card>> getAllCards(Long roomId) {
        String redisKey = generateSelectedFiveCardKey(roomId);
        return cardRedisOps.opsForList()
                .range(redisKey, 0, -1)  // 전체 리스트 반환
                .map(Card::valueOf)
                .collectList();
    }

    public Mono<Void> savePlayerSelectedCard(Long roomId, ChooseLeadPlayer chooseLeadPlayer) {
        String redisKey = generatePlayerSelectedCardKey(roomId);
        return chooseLeadPlayerRedisOps.opsForValue().set(redisKey, chooseLeadPlayer)
                .then();
    }

    public Mono<ChooseLeadPlayer> getPlayerSelectedCard(Long roomId) {
        String redisKey = generatePlayerSelectedCardKey(roomId);
        return chooseLeadPlayerRedisOps.opsForValue().get(redisKey)
                .defaultIfEmpty(new ChooseLeadPlayer());
    }
}
