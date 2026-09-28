package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.ChooseLeadPlayer;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import reactor.core.publisher.Mono;

import java.util.List;

public interface LeadingPlayerRepository {
    Mono<Void> cleanup(long roomId);
    Mono<Void> saveSelectedCard(List<Card> cards, Long roomId);
    Mono<Card> getCardByIndex(Long roomId, int index);
    Mono<List<Card>> getAllCards(Long roomId);
    Mono<Void> savePlayerMonth(Long roomId, Player player, int month);
    Mono<ChooseLeadPlayer> getPlayerSelectedCard(Long roomId);
    /** Redis는 원자적 선점, in-memory는 무변경 true. 선택 완료 확인 뒤 게임 락 안에서만 호출한다. */
    Mono<Boolean> tryClaimLeaderSelectionTrigger(Long roomId);
}
