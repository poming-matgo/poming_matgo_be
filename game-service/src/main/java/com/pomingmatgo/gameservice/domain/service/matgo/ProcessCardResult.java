package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

@Getter
@Builder
public class ProcessCardResult {
    private final List<Card> acquiredCards;
    private final boolean choiceRequired;

    public static ProcessCardResult immediate(List<Card> cards) {
        return ProcessCardResult.builder()
                .acquiredCards(cards)
                .choiceRequired(false)
                .build();
    }

    public static ProcessCardResult choicePending() {
        return ProcessCardResult.builder()
                .acquiredCards(Collections.emptyList())
                .choiceRequired(true)
                .build();
    }
}
