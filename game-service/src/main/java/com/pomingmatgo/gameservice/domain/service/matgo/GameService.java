package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.ScoreCardRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;


@Service
@RequiredArgsConstructor
public class GameService {
    private final InstalledCardRepository installedCardRepository;
    private final ScoreCardRepository scoreCardRepository;
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
                        processSubmittedCard(roomId, submittedCard, turnedCard)
                ));
    }

    private Flux<Card> processSubmittedCard(long roomId, Card submittedCard, Card turnedCard) {
        int turnedCardMonth = turnedCard.getMonth();
        int submittedCardMonth = submittedCard.getMonth();

        if (turnedCardMonth == submittedCardMonth) {
            return handleSameMonthCards(roomId, submittedCard, turnedCard);
        } else {
            return handleDifferentMonthCards(roomId, submittedCard, turnedCard);
        }
    }

    private Flux<Card> handleSameMonthCards(long roomId, Card submittedCard, Card turnedCard) {
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

    private Flux<Card> handleDifferentMonthCards(long roomId, Card submittedCard, Card turnedCard) {
        return Flux.merge(
                processCardByMonth(roomId, submittedCard),
                processCardByMonth(roomId, turnedCard)
        );
    }

    private Flux<Card> processCardByMonth(long roomId, Card card) {
        int month = card.getMonth();
        return installedCardRepository.getRevealedCardByMonth(roomId, month)
                .collectList()
                .flatMapMany(cardStack -> {
                    if (cardStack.isEmpty()) {
                        return installedCardRepository.saveRevealedCard(List.of(card), roomId)
                                .thenMany(Flux.empty());
                    } else if (cardStack.size() == 1) {
                        return installedCardRepository.deleteAllRevealedCardByMonth(roomId, month)
                                .thenMany(Flux.fromIterable(cardStack).concatWith(Flux.just(card)));
                    }
                    return Flux.empty();
                });
    }

    /*public Mono<Card> moveCardPlayerToPlayer(long toPlayerNum, long fromPlayerNum, long roomId) {
        Flux<Card> cards = scoreCardRepository.getPiCards(roomId, fromPlayerNum).cache();

        return cards.filter(card -> card.getSpecialType() == null)
                .next()
                .switchIfEmpty(
                        cards.filter(card -> card.getSpecialType() == SpecialType.SSANG_PI)
                                .next()
                );
        //todo: toPlayer에 save 추가해야함
    }*/
}
