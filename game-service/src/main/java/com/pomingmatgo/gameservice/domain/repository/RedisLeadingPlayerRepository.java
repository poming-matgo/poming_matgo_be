package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.ChooseLeadPlayer;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import org.springframework.context.annotation.Profile;

import java.util.List;

@Profile("redis")
@Repository
public class RedisLeadingPlayerRepository implements LeadingPlayerRepository {

    private final ReactiveRedisOperations<String, String> cardRedisOps;

    public RedisLeadingPlayerRepository(@Qualifier("cardRedisTemplate") ReactiveRedisOperations<String, String> cardRedisOps) {
        this.cardRedisOps = cardRedisOps;
    }

    private static final String SELECTED_FIVE_CARD_KEY_FORMAT = "game:%d:lead:select5";
    private static final String PLAYER1_MONTH_KEY_FORMAT = "game:%d:lead:p1Month";
    private static final String PLAYER2_MONTH_KEY_FORMAT = "game:%d:lead:p2Month";
    private static final String LEADER_TRIGGER_KEY_FORMAT = "game:%d:lead:trigger";

    private String generateSelectedFiveCardKey(long roomId) {
        return String.format(SELECTED_FIVE_CARD_KEY_FORMAT, roomId);
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
                .range(redisKey, 0, -1)
                .map(Card::valueOf)
                .collectList();
    }

    public Mono<Void> savePlayerMonth(Long roomId, Player player, int month) {
        String key = player == Player.PLAYER_1
                ? String.format(PLAYER1_MONTH_KEY_FORMAT, roomId)
                : String.format(PLAYER2_MONTH_KEY_FORMAT, roomId);
        return cardRedisOps.opsForValue().set(key, String.valueOf(month)).then();
    }

    public Mono<ChooseLeadPlayer> getPlayerSelectedCard(Long roomId) {
        String p1Key = String.format(PLAYER1_MONTH_KEY_FORMAT, roomId);
        String p2Key = String.format(PLAYER2_MONTH_KEY_FORMAT, roomId);
        return Mono.zip(
                cardRedisOps.opsForValue().get(p1Key).defaultIfEmpty("0"),
                cardRedisOps.opsForValue().get(p2Key).defaultIfEmpty("0")
        ).map(tuple -> new ChooseLeadPlayer(
                Integer.parseInt(tuple.getT1()),
                Integer.parseInt(tuple.getT2())
        ));
    }

    public Mono<Boolean> tryClaimLeaderSelectionTrigger(Long roomId) {
        String key = String.format(LEADER_TRIGGER_KEY_FORMAT, roomId);
        // TTL 없음. InMemory 영구 저장과 동일 의미 — cleanup(roomId)에서 명시적 삭제 보장
        return cardRedisOps.opsForValue().setIfAbsent(key, "1");
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        String select5Key = generateSelectedFiveCardKey(roomId);
        String p1MonthKey = String.format(PLAYER1_MONTH_KEY_FORMAT, roomId);
        String p2MonthKey = String.format(PLAYER2_MONTH_KEY_FORMAT, roomId);
        String triggerKey = String.format(LEADER_TRIGGER_KEY_FORMAT, roomId);
        return cardRedisOps.delete(select5Key, p1MonthKey, p2MonthKey, triggerKey).then();
    }
}
