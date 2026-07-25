package com.pomingmatgo.gameservice.domain.service.matgo.calculatescore;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.card.CardType;
import com.pomingmatgo.gameservice.domain.card.SpecialType;
import com.pomingmatgo.gameservice.domain.score.ScoreBreakdown;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public final class ScoreCalculator {
    public int calculatePiScore(List<Card> cards) {
        int piCnt = countPi(cards);
        return piCnt < 10 ? 0 : piCnt - 9;
    }

    /** 쌍피는 2장으로 센다 — 피 점수와 피박 판정이 같은 수를 쓴다 */
    public int countPi(List<Card> cards) {
        return cards.stream()
                .mapToInt(card -> {
                    if (!CardType.PI.equals(card.getType())) {
                        throw new IllegalArgumentException("피 카드가 아닙니다.");
                    }
                    return card.getSpecialType() == SpecialType.SSANG_PI ? 2 : 1;
                })
                .sum();
    }

    public int calculateGwangScore(List<Card> cards) {
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
    }

    public int calculateKkutScore(List<Card> cards) {
        int size = cards.size();
        boolean isGodori = cards.stream()
                .filter(card -> card.getSpecialType() == SpecialType.GODORI)
                .limit(3)
                .count() >= 3;
        int godoriScore = isGodori ? 5 : 0;
        return size < 5 ? godoriScore : godoriScore + size - 4;
    }

    public int calculateDdiScore(List<Card> cards) {
        long size = cards.size();
        Map<SpecialType, Long> specialTypeCount = cards.stream()
                .filter(card -> card.getSpecialType() != null)
                .collect(Collectors.groupingBy(Card::getSpecialType, Collectors.counting()));
        int additionalScore = (int) Stream.of(SpecialType.HONG_DAN, SpecialType.CHO_DAN, SpecialType.CHUNG_DAN)
                .filter(type -> specialTypeCount.getOrDefault(type, 0L) >= 3)
                .count() * 3;
        return size < 5 ? additionalScore : additionalScore + (int) size - 4;
    }

    public ScoreBreakdown calculate(List<Card> cards) {
        List<Card> piCards = byType(cards, CardType.PI);
        List<Card> gwangCards = byType(cards, CardType.GWANG);
        List<Card> kkutCards = byType(cards, CardType.KKUT);
        List<Card> ddiCards = byType(cards, CardType.DDI);

        return ScoreBreakdown.builder()
                .piScore(calculatePiScore(piCards))
                .gwangScore(calculateGwangScore(gwangCards))
                .kkutScore(calculateKkutScore(kkutCards))
                .ddiScore(calculateDdiScore(ddiCards))
                .piCount(countPi(piCards))
                .gwangCount(gwangCards.size())
                .kkutCount(kkutCards.size())
                .ddiCount(ddiCards.size())
                .build();
    }

    private List<Card> byType(List<Card> cards, CardType type) {
        return cards.stream().filter(c -> c.getType() == type).toList();
    }
}
