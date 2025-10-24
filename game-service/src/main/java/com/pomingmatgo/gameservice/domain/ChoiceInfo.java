package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;

@Getter
@Builder
public class ChoiceInfo implements Serializable {
    private int playerNumToChoose;
    private Card submittedCard;
    private List<Card> selectableCards;
}
