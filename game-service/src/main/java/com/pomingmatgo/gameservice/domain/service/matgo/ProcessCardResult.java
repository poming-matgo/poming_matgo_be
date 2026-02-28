package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
@Builder(toBuilder = true)
public class ProcessCardResult {

    @Builder.Default
    private final List<Card> acquiredCards = new ArrayList<>();

    @Builder.Default
    private final List<SpecialEvent> specialEvents = new ArrayList<>();

    private final boolean choiceRequired;

    private final boolean claimOpponentPi;

    @Builder.Default
    private final List<Card> moveCards = new ArrayList<>();

    public ProcessCardResult merge(ProcessCardResult other) {
        if (other == null) return this;

        List<Card> mergedAcquired = new ArrayList<>(this.acquiredCards);
        mergedAcquired.addAll(other.getAcquiredCards() != null ? other.getAcquiredCards() : Collections.emptyList());

        List<SpecialEvent> mergedEvents = new ArrayList<>(this.specialEvents);
        mergedEvents.addAll(other.getSpecialEvents() != null ? other.getSpecialEvents() : Collections.emptyList());

        List<Card> mergedMoveCards = new ArrayList<>(this.moveCards);
        mergedMoveCards.addAll(other.getMoveCards() != null ? other.getMoveCards() : Collections.emptyList());

        return this.toBuilder()
                .acquiredCards(mergedAcquired)
                .specialEvents(mergedEvents)
                .moveCards(mergedMoveCards)
                .claimOpponentPi(this.claimOpponentPi || other.isClaimOpponentPi()) // 둘 중 하나라도 피를 뺏으면 true
                .choiceRequired(this.choiceRequired || other.isChoiceRequired())
                .build();
    }


    public static ProcessCardResult immediate(List<Card> cards) {
        return ProcessCardResult.builder()
                .acquiredCards(cards != null ? cards : new ArrayList<>())
                .choiceRequired(false)
                .claimOpponentPi(false)
                .build();
    }

    public static ProcessCardResult choicePending(List<Card> cards) {
        return ProcessCardResult.builder()
                .acquiredCards(cards != null ? cards : new ArrayList<>())
                .choiceRequired(true)
                .claimOpponentPi(false)
                .build();
    }

    public static ProcessCardResult claimOpponentPi(List<Card> cards, Card _moveCard) {
        return ProcessCardResult.builder()
                .acquiredCards(cards != null ? cards : new ArrayList<>())
                .choiceRequired(false)
                .claimOpponentPi(true)
                .moveCards(_moveCard != null ? List.of(_moveCard) : new ArrayList<>())
                .build();
    }

    public static ProcessCardResult ppeok(List<Card> cards) {
        return ProcessCardResult.builder()
                .acquiredCards(cards != null ? cards : new ArrayList<>())
                .choiceRequired(false)
                .claimOpponentPi(false)
                .specialEvents(List.of(SpecialEvent.PPEOK))
                .build();
    }

    public static ProcessCardResult ttadak(List<Card> cards, Card _moveCard) {
        return ProcessCardResult.builder()
                .acquiredCards(cards != null ? cards : new ArrayList<>())
                .choiceRequired(false)
                .claimOpponentPi(true)
                .specialEvents(List.of(SpecialEvent.TTADAK))
                .moveCards(_moveCard != null ? List.of(_moveCard) : new ArrayList<>())
                .build();
    }

    public static ProcessCardResult jjok(List<Card> cards, Card _moveCard) {
        return ProcessCardResult.builder()
                .acquiredCards(cards != null ? cards : new ArrayList<>())
                .choiceRequired(false)
                .claimOpponentPi(true)
                .specialEvents(List.of(SpecialEvent.JJOK))
                .moveCards(_moveCard != null ? List.of(_moveCard) : new ArrayList<>())
                .build();
    }
}
