package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.data.redis.core.RedisHash;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
@RedisHash(value = "installedCard")
public class InstalledCard {
    private static final long ALL_CARDS_OF_MONTH = 4;

    private List<Card> player1;
    private List<Card> player2;
    private List<Card> revealedCard;
    private List<Card> hiddenCard;

    /** 초기 바닥에 같은 월 4장 — 그 월을 아무도 먹을 수 없어 판이 성립하지 않는다(무승부) */
    public boolean hasFourOfSameMonthOnFloor() {
        return revealedCard.stream()
                .collect(Collectors.groupingBy(Card::getMonth, Collectors.counting()))
                .containsValue(ALL_CARDS_OF_MONTH);
    }
}
