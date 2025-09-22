package com.pomingmatgo.gameservice.api.response.websocket;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class AcquiredCardRes {
    List<Card> acquiredCards;
    int score;
}
