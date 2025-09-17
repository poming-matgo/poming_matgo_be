package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.data.redis.core.RedisHash;

import java.util.ArrayList;
import java.util.LinkedList;

@Getter
@AllArgsConstructor
@RedisHash(value = "installedCard")
public class InstalledCard {
    private LinkedList<Card> player1;
    private LinkedList<Card> player2;
    private ArrayList<Card> revealedCard;
    private ArrayList<Card> hiddenCard;
}
