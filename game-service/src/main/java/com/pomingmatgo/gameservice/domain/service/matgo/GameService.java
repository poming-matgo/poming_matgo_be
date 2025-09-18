package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


@Service
@RequiredArgsConstructor
public class GameService {
    private final InstalledCardRepository installedCardRepository;
    public Mono<Boolean> isConfusedPlayer(long roomId, int playerNum) {
        Flux<Card> cardFlux = (playerNum == 1)
                ? installedCardRepository.getPlayer1Cards(roomId)
                : installedCardRepository.getPlayer2Cards(roomId);

        return cardFlux
                .groupBy(Card::getMonth)
                .flatMap(group -> group.count().map(count -> count == 4))
                .any(Boolean::booleanValue);
    }

    /*public Flux<Card> submitCard(long roomId, int playerNum, Mono<Card> card) {

    }*/
}
