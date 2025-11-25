package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Builder(toBuilder = true)
public class ProcessCardResult {
    private final List<Card> acquiredCards;
    private final boolean ppeok; // 뻑
    private final boolean choiceRequired;
    private final boolean claimOpponentPi;
    private final Card moveCard;

    public static ProcessCardResult immediate(List<Card> cards) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .choiceRequired(false)
                .claimOpponentPi(false)
                .ppeok(false)
                .build();
    }

    public static ProcessCardResult choicePending(List<Card> cards) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .choiceRequired(true)
                .claimOpponentPi(false)
                .ppeok(false)
                .build();
    }

    public static ProcessCardResult claimOpponentPi(List<Card> cards, Card _moveCard) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .choiceRequired(false)
                .claimOpponentPi(true)
                .ppeok(false)
                .moveCard(_moveCard)
                .build();
    }

    public static ProcessCardResult ppeok(List<Card> cards) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .choiceRequired(false)
                .claimOpponentPi(false)
                .ppeok(true)
                .build();
    }
}
