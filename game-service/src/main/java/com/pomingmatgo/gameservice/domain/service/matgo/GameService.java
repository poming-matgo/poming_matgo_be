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
                .flatMapMany(turnedCard -> submittedCardMono.flatMapMany(submittedCard ->
                        processSubmittedCard(roomId, turnedCard, submittedCard)
                ));
    }
    private Flux<Card> processSubmittedCard(long roomId, Card turnedCard, Card submittedCard) {
        int turnedCardMonth = turnedCard.getMonth();
        int submittedCardMonth = submittedCard.getMonth();

        if (turnedCardMonth == submittedCardMonth) {
            return handleSameMonthCards(roomId, turnedCard, submittedCard);
        } else {
            return handleDifferentMonthCards(roomId, turnedCard, submittedCard);
        }
    }

    private Flux<Card> handleSameMonthCards(long roomId, Card turnedCard, Card submittedCard) {
        int month = turnedCard.getMonth();
        return installedCardRepository.getRevealedCardByMonth(roomId, month)
                .collectList()
                .flatMapMany(cardStack -> {
                    if (cardStack.size() != 1) {
                        return installedCardRepository.deleteAllRevealedCardByMonth(roomId, month)
                                .thenMany(Flux.fromIterable(List.of(turnedCard, submittedCard)).concatWith(Flux.fromIterable(cardStack)));
                    } else {
                        //뻑
                        return installedCardRepository.saveRevealedCard(List.of(turnedCard, submittedCard), roomId)
                                .thenMany(Flux.empty());
                    }
                });
    }

    private Flux<Card> handleDifferentMonthCards(long roomId, Card turnedCard, Card submittedCard) {
        int turnedMonth = turnedCard.getMonth();
        int submittedMonth = submittedCard.getMonth();
        return Flux.merge(
                installedCardRepository.getRevealedCardByMonth(roomId, turnedMonth),
                installedCardRepository.getRevealedCardByMonth(roomId, submittedMonth),
                Flux.just(turnedCard, submittedCard)
        );
    }

}
