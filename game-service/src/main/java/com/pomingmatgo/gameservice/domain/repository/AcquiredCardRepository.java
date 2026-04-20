package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.card.Card;
import reactor.core.publisher.Mono;

import java.util.List;

public interface AcquiredCardRepository {
    Mono<Long> addCards(long roomId, long playerId, List<Card> cards);
    Mono<List<Card>> getAllCards(long roomId, long playerId);
    Mono<Long> removeCard(long roomId, long playerId, Card card);
}
