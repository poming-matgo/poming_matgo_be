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

public final class ScoreCalculator {
    private ScoreCalculator() {}
    public static Mono<Integer> calculatePiScore(Flux<Card> cardFlux) {
        return cardFlux.reduce(0, (piCnt, card) -> {
            if (!CardType.PI.equals(card.getType())) {
                throw new IllegalArgumentException("피 카드가 아닙니다.");
            }
            return piCnt + (Objects.equals(card.getSpecialType(), SpecialType.SSANG_PI) ? 2 : 1);
        }).map(piCnt -> piCnt < 10 ? 0 : piCnt - 9);
    }

    public static Mono<Integer> calculateGwangScore(Flux<Card> cardFlux) {
        return cardFlux.collectList()
                .map(cards -> {
                    int size = cards.size();

                    return switch (size) {
                        case 3 -> {
                            boolean hasBiGwang = cards.stream()
                                    .anyMatch(card -> Objects.equals(card.getSpecialType(), SpecialType.BI_GWANG));
                            yield hasBiGwang ? 2 : 3;
                        }
                        case 4 -> 4;
                        case 5 -> 15;
                        default -> 0;
                    };
                });
    }

    public static Mono<Integer> calculateKkutScore(Flux<Card> cardFlux) {
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

    public static Mono<Integer> calculateDdiScore(Flux<Card> cardFlux) {
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

    public static Mono<Integer> calculateTotalScore(Flux<Card> cardFlux) {
        Mono<Integer> piScore = calculatePiScore(cardFlux.filter(card -> card.getType() == CardType.PI));
        Mono<Integer> gwangScore = calculateGwangScore(cardFlux.filter(card -> card.getType() == CardType.GWANG));
        Mono<Integer> kkutScore = calculateKkutScore(cardFlux.filter(card -> card.getType() == CardType.KKUT));
        Mono<Integer> ddiScore = calculateDdiScore(cardFlux.filter(card -> card.getType() == CardType.DDI));

        return Mono.zip(piScore, gwangScore, kkutScore, ddiScore)
                .map(tuple -> tuple.getT1() + tuple.getT2() + tuple.getT3() + tuple.getT4());
    }
}