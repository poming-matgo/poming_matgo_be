package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;


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

    public Flux<Card> submitCard(long roomId, Mono<Card> submittedCardMono) {
        return installedCardRepository.getTopCard(roomId)
                .zipWith(submittedCardMono)
                .flatMapMany(tuple -> {
                    Card turnedCard = tuple.getT1();
                    Card submittedCard = tuple.getT2();
                    int turnedCardMonth = turnedCard.getMonth();
                    int submittedCardMonth = submittedCard.getMonth();

                    if (turnedCardMonth == submittedCardMonth) {
                        return installedCardRepository.getRevealedCardByMonth(roomId, turnedCardMonth)
                                .collectList()
                                .flatMapMany(cardStack -> {
                                    if (cardStack.size() != 1) {
                                        return installedCardRepository.deleteAllRevealedCardByMonth(roomId, turnedCardMonth)
                                                .flatMapMany(deleted -> {
                                                    cardStack.add(turnedCard);
                                                    cardStack.add(submittedCard);
                                                    return Flux.fromIterable(cardStack);
                                                });

                                    } else {
                                        //뻑
                                        List<Card> addCard = List.of(turnedCard, submittedCard);
                                        return installedCardRepository.saveRevealedCard(addCard, roomId)
                                                .flatMapMany(deleted -> Flux.empty());
                                    }
                                });
                    } else {
                        return Flux.merge(
                                installedCardRepository.getRevealedCardByMonth(roomId, turnedCardMonth),
                                installedCardRepository.getRevealedCardByMonth(roomId, submittedCardMonth),
                                Flux.just(turnedCard),
                                Flux.just(submittedCard)
                        );
                    }
                });
    }
}
