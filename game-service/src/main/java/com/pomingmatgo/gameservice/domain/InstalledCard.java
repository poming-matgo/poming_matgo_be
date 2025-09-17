package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.data.redis.core.RedisHash;

import java.util.List;

@Getter
@AllArgsConstructor
@RedisHash(value = "installedCard")
public class InstalledCard {
    private List<Card> player1;
    private List<Card> player2;
    private List<Card> revealedCard;
    private List<Card> hiddenCard;
}
