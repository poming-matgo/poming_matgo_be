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
        // 선택 대기면 this(앞선 획득분)를 버려도 된다 — CardMatchEngine이 choiceInfo.prev*로 이월해 뒀다
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

    /** 판쓸이 보상을 이미 확정된 이번 턴 획득분 위에 얹는다 */
    public ProcessCardResult withSweep(Card pi) {
        List<Card> acquired = new ArrayList<>(this.acquiredCards);
        acquired.add(pi);

        List<SpecialEvent> events = new ArrayList<>(this.specialEvents);
        events.add(SpecialEvent.SWEEP);

        List<Card> moves = new ArrayList<>(this.moveCards);
        moves.add(pi);

        return this.toBuilder()
                .acquiredCards(acquired)
                .specialEvents(events)
                .moveCards(moves)
                .claimOpponentPi(true)
                .build();
    }

    public static ProcessCardResult immediate(List<Card> cards) {
        // 호출자 리스트를 그대로 참조하지 않도록 복사 (choicePending과 동일한 방어)
        return ProcessCardResult.builder()
                .acquiredCards(new ArrayList<>(cards))
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
