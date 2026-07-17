package com.pomingmatgo.gameservice.domain.service.matgo.calculatescore;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.card.CardType;
import com.pomingmatgo.gameservice.domain.card.SpecialType;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public final class ScoreCalculator {
    public int calculatePiScore(List<Card> cards) {
        int piCnt = cards.stream()
                .mapToInt(card -> {
                    if (!CardType.PI.equals(card.getType())) {
                        throw new IllegalArgumentException("피 카드가 아닙니다.");
                    }
                    return card.getSpecialType() == SpecialType.SSANG_PI ? 2 : 1;
                })
                .sum();
        return piCnt < 10 ? 0 : piCnt - 9;
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

    public int calculateTotalScore(List<Card> cards) {
        List<Card> piCards = cards.stream().filter(c -> c.getType() == CardType.PI).toList();
        List<Card> gwangCards = cards.stream().filter(c -> c.getType() == CardType.GWANG).toList();
        List<Card> kkutCards = cards.stream().filter(c -> c.getType() == CardType.KKUT).toList();
        List<Card> ddiCards = cards.stream().filter(c -> c.getType() == CardType.DDI).toList();

        int piScore = calculatePiScore(piCards);
        int gwangScore = calculateGwangScore(gwangCards);
        int kkutScore = calculateKkutScore(kkutCards);
        int ddiScore = calculateDdiScore(ddiCards);

        return piScore + gwangScore + kkutScore + ddiScore;
    }
}
