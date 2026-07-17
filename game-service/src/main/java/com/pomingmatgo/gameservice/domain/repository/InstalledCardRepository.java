package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import reactor.core.publisher.Mono;

import java.util.List;

public interface InstalledCardRepository {
    Mono<Void> cleanup(long roomId);
    Mono<Boolean> savePlayerCards(List<Card> cards, long roomId, Player player);
    Mono<Boolean> deletePlayerCards(long roomId, Player player);
    Mono<Boolean> saveRevealedCard(List<Card> cards, long roomId);
    Mono<Boolean> saveHiddenCard(List<Card> cards, long roomId);
    Mono<Boolean> deleteAllRevealedCardByMonth(long roomId, int month);
    Mono<Boolean> deleteRevealedCard(long roomId, Card card);
    Mono<List<Card>> getRevealedCardByMonth(long roomId, int month);
    Mono<List<Card>> getAllRevealedCards(long roomId);
    /** 더미 맨 위 카드를 꺼낸다(소모). 더미가 비어 있으면 empty */
    Mono<Card> drawTopCard(long roomId);
    Mono<List<Card>> getPlayerCards(long roomId, Player player);
    Mono<Void> updatePlayerCards(long roomId, Player player, List<Card> cards);
}
