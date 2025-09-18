package com.pomingmatgo.gameservice.domain.service.matgo.calculatescore;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.card.CardType;
import com.pomingmatgo.gameservice.domain.card.SpecialType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

public class ScoreCalculator {
    public Mono<Integer> calculatePiScore(Flux<Card> cardFlux) {
        return cardFlux.reduce(0, (piCnt, card) -> {
            if (!CardType.PI.equals(card.getType())) {
                throw new IllegalArgumentException("피 카드가 아닙니다.");
            }
            return piCnt + (Objects.equals(card.getSpecialType(), SpecialType.SSANG_PI) ? 2 : 1);
        }).map(piCnt -> piCnt < 10 ? 0 : piCnt - 9);
    }
}