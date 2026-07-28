package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.card.Card;
import reactor.core.publisher.Mono;

import java.util.List;

public interface AcquiredCardRepository {
    Mono<Long> addCards(long roomId, long playerId, List<Card> cards);
    /** 반환 순서는 Card natural order 고정 — 피 뺏기 tie-break가 이 순서에 의존한다 (replay 결정성) */
    Mono<List<Card>> getAllCards(long roomId, long playerId);
    Mono<Long> removeCard(long roomId, long playerId, Card card);
    Mono<Void> cleanup(long roomId);
}
