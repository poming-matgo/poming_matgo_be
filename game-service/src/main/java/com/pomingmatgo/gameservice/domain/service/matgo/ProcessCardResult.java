package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Builder(toBuilder = true)
public class ProcessCardResult {

    @Builder.Default
    private final List<Card> acquiredCards = new ArrayList<>();

    @Builder.Default
    private final List<SpecialEvent> specialEvents = new ArrayList<>();

    @Builder.Default
    private final List<Card> selectableCards = new ArrayList<>();

    private final boolean choiceRequired;

    private final boolean claimOpponentPi;

    @Builder.Default
    private final List<Card> moveCards = new ArrayList<>();

    public ProcessCardResult merge(ProcessCardResult other) {
        // 선택 대기 결과면 그대로 반환 — this(앞선 획득분)는 버려지지만, handleTwoCardsOnFloor가
        // choiceInfo.prev*로 이월해 두었으므로 선택 완료 시 finalizeTurn이 복원한다
        if (other.isChoiceRequired()) return other;

        List<Card> mergedAcquired = new ArrayList<>(this.acquiredCards);
        mergedAcquired.addAll(other.acquiredCards);

        List<SpecialEvent> mergedEvents = new ArrayList<>(this.specialEvents);
        mergedEvents.addAll(other.specialEvents);

        List<Card> mergedMoveCards = new ArrayList<>(this.moveCards);
        mergedMoveCards.addAll(other.moveCards);

        return this.toBuilder()
                .acquiredCards(mergedAcquired)
                .specialEvents(mergedEvents)
                .moveCards(mergedMoveCards)
                .claimOpponentPi(this.claimOpponentPi || other.isClaimOpponentPi()) // 둘 중 하나라도 피를 뺏으면 true
                .build();
    }

    public static ProcessCardResult immediate(List<Card> cards) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .build();
    }

    public static ProcessCardResult choicePending(List<Card> cards) {
        return ProcessCardResult.builder()
                .selectableCards(new ArrayList<>(cards))
                .choiceRequired(true)
                .build();
    }

    public static ProcessCardResult claimOpponentPi(List<Card> cards, Card moveCard) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .claimOpponentPi(true)
                .moveCards(List.of(moveCard))
                .build();
    }

    public static ProcessCardResult ppeok(List<Card> cards) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .specialEvents(List.of(SpecialEvent.PPEOK))
                .build();
    }

    public static ProcessCardResult ttadak(List<Card> cards, Card moveCard) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .claimOpponentPi(true)
                .specialEvents(List.of(SpecialEvent.TTADAK))
                .moveCards(List.of(moveCard))
                .build();
    }

    public static ProcessCardResult jjok(List<Card> cards, Card moveCard) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .claimOpponentPi(true)
                .specialEvents(List.of(SpecialEvent.JJOK))
                .moveCards(List.of(moveCard))
                .build();
    }
}
