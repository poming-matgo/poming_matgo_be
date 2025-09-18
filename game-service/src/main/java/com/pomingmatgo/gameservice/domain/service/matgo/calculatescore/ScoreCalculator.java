package com.pomingmatgo.gameservice.domain.service.matgo.calculatescore;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.card.CardType;
import com.pomingmatgo.gameservice.domain.card.SpecialType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ScoreCalculator {
    public Mono<Integer> calculatePiScore(Flux<Card> cardFlux) {
        return cardFlux.reduce(0, (piCnt, card) -> {
            if (!CardType.PI.equals(card.getType())) {
                throw new IllegalArgumentException("피 카드가 아닙니다.");
            }
            return piCnt + (Objects.equals(card.getSpecialType(), SpecialType.SSANG_PI) ? 2 : 1);
        }).map(piCnt -> piCnt < 10 ? 0 : piCnt - 9);
    }

    public Mono<Integer> calculateGwangScore(Flux<Card> cardFlux) {
        return cardFlux.collectList()
                .map(cards -> {
                    int size = cards.size();
                    boolean hasBiGwang = cards.stream()
                            .anyMatch(card -> Objects.equals(card.getSpecialType(), SpecialType.BI_GWANG));

                    return switch (size) {
                        case 3 -> hasBiGwang ? 2 : 3;
                        case 4 -> 4;
                        case 5 -> 15;
                        default -> 0;
                    };
                });
    }

    public Mono<Integer> calculateKkutScore(Flux<Card> cardFlux) {
        return cardFlux.collectList()
                .map(cards -> {
                    int size = cards.size();
                    boolean isGodori = cards.stream()
                            .filter(card -> card.getSpecialType() == SpecialType.GODORI)
                            .limit(3)
                            .count() >= 3;

                    int godoriScore = isGodori ? 5 : 0;

                    return size < 5 ? godoriScore : godoriScore + size - 4;
                });
    }

    public Mono<Integer> calculateDdiScore(Flux<Card> cardFlux) {
        return cardFlux.collectList()
                .map(cards -> {
                    long size = cards.size();

                    Map<SpecialType, Long> specialTypeCount = cards.stream()
                            .filter(card -> card.getSpecialType() != null)
                            .collect(Collectors.groupingBy(Card::getSpecialType, Collectors.counting()));

                    int additionalScore = (int) Stream.of(SpecialType.HONG_DAN, SpecialType.CHO_DAN, SpecialType.CHUNG_DAN)
                            .filter(type -> specialTypeCount.getOrDefault(type, 0L) >= 3)
                            .count() * 3;

                    return size < 5 ? additionalScore : additionalScore + (int) size - 4;
                });
    }
}