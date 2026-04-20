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
    Mono<Boolean> tryClaimLeaderSelectionTrigger(Long roomId);
}
