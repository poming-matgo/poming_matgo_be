package com.pomingmatgo.gameservice.domain.messaging;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class LeadSelectionRes {
    private int player1Month;
    private int player2Month;
    private int leadPlayer;

    private List<Card> fiveCards;
}
