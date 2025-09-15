package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.card.Card;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class GameService {
    private static final Random RANDOM = new Random();

    @Qualifier("cardRedisTemplate")
    @Autowired
    private ReactiveRedisOperations<String, Card> redisOps;

    //선 플레이어 정하는 과정
    public Mono<Void> pickFiveCardsAndSave() {
        Map<Integer, List<Card>> cardsByMonth = Arrays.stream(Card.values())
                .collect(Collectors.groupingBy(Card::getMonth));

        List<Integer> months = new ArrayList<>(cardsByMonth.keySet());
        Collections.shuffle(months, RANDOM);
        List<Integer> selectedMonths = months.subList(0, 5);

        List<Card> selectedCards = selectedMonths.stream()
                .map(month -> {
                    List<Card> cards = cardsByMonth.get(month);
                    return cards.get(RANDOM.nextInt(cards.size()));
                })
                .toList();

        String redisKey = "selectedCards";
        return redisOps.opsForList()
                .rightPushAll(redisKey, selectedCards)
                .then();
    }
}
