package com.pomingmatgo.gameservice.domain.service.matgo.calculatescore;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.card.CardType;
import com.pomingmatgo.gameservice.domain.card.SpecialType;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ScoreCalculator {
    private ScoreCalculator() {}

    public static Mono<Integer> calculatePiScore(List<Card> cards) {
        int piCnt = cards.stream().reduce(0, (acc, card) -> {
            if (!CardType.PI.equals(card.getType())) {
                throw new IllegalArgumentException("피 카드가 아닙니다.");
            }
            return acc + (Objects.equals(card.getSpecialType(), SpecialType.SSANG_PI) ? 2 : 1);
        }, Integer::sum);
        int result = piCnt < 10 ? 0 : piCnt - 9;
        return Mono.just(result);
    }

    public static Mono<Integer> calculateGwangScore(List<Card> cards) {
        int size = cards.size();
        int score = switch (size) {
            case 3 -> {
                boolean hasBiGwang = cards.stream()
                        .anyMatch(card -> Objects.equals(card.getSpecialType(), SpecialType.BI_GWANG));
                yield hasBiGwang ? 2 : 3;
            }
            case 4 -> 4;
            case 5 -> 15;
            default -> 0;
        };
        return Mono.just(score);
    }

    public static Mono<Integer> calculateKkutScore(List<Card> cards) {
        int size = cards.size();
        boolean isGodori = cards.stream()
                .filter(card -> card.getSpecialType() == SpecialType.GODORI)
                .limit(3)
                .count() >= 3;
        int godoriScore = isGodori ? 5 : 0;
        int score = size < 5 ? godoriScore : godoriScore + size - 4;
        return Mono.just(score);
    }

    public static Mono<Integer> calculateDdiScore(List<Card> cards) {
        long size = cards.size();
        Map<SpecialType, Long> specialTypeCount = cards.stream()
                .filter(card -> card.getSpecialType() != null)
                .collect(Collectors.groupingBy(Card::getSpecialType, Collectors.counting()));
        int additionalScore = (int) Stream.of(SpecialType.HONG_DAN, SpecialType.CHO_DAN, SpecialType.CHUNG_DAN)
                .filter(type -> specialTypeCount.getOrDefault(type, 0L) >= 3)
                .count() * 3;
        int score = size < 5 ? additionalScore : additionalScore + (int) size - 4;
        return Mono.just(score);
    }

    public static Mono<Integer> calculateTotalScore(List<Card> cards) {
        List<Card> piCards = cards.stream()
                .filter(card -> card.getType() == CardType.PI)
                .toList();
        List<Card> gwangCards = cards.stream()
                .filter(card -> card.getType() == CardType.GWANG)
                .toList();
        List<Card> kkutCards = cards.stream()
                .filter(card -> card.getType() == CardType.KKUT)
                .toList();
        List<Card> ddiCards = cards.stream()
                .filter(card -> card.getType() == CardType.DDI)
                .toList();

        Mono<Integer> piScore = calculatePiScore(piCards);
        Mono<Integer> gwangScore = calculateGwangScore(gwangCards);
        Mono<Integer> kkutScore = calculateKkutScore(kkutCards);
        Mono<Integer> ddiScore = calculateDdiScore(ddiCards);

        return Mono.zip(piScore, gwangScore, kkutScore, ddiScore)
                .map(tuple -> tuple.getT1() + tuple.getT2() + tuple.getT3() + tuple.getT4());
    }
}
