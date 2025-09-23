package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.ScoreCardRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALUD_CARD;


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

    public Mono<Card> getTopCard(long roomId) {
        return installedCardRepository.getTopCard(roomId);
    }

    public Mono<Card> submitCardEvent(long roomId, RequestEvent<NormalSubmitReq> event) {
        int playerNum = event.getPlayerNum();
        int cardIndex = event.getData().getCardIndex();

        Mono<List<Card>> playerCardsMono = (playerNum == 1)
                ? installedCardRepository.getPlayer1Cards(roomId).collectList()
                : installedCardRepository.getPlayer2Cards(roomId).collectList();

        return playerCardsMono.flatMap(playerCards -> {
            if (cardIndex < 0 || cardIndex >= playerCards.size()) {
                throw new WebSocketBusinessException(INVALUD_CARD);
            }

            Card submittedCard = playerCards.remove(cardIndex);

            return (playerNum == 1
                    ? installedCardRepository.deletePlayer1Card(roomId)
                    .flatMap(ignored -> installedCardRepository.savePlayer1Card(playerCards, roomId))
                    : installedCardRepository.deletePlayer2Card(roomId)
                    .flatMap(ignored -> installedCardRepository.savePlayer2Card(playerCards, roomId)))
                    .thenReturn(submittedCard);
        });
    }


    public Flux<Card> submitCard(long roomId, Card submittedCard, Card turnedCard) {
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
                        //todo: 다른 사람 카드 가져오는 로직 추가
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
                    int size = cardStack.size();
                    switch (size) {
                        case 0:
                            return installedCardRepository.saveRevealedCard(List.of(card), roomId)
                                    .thenMany(Flux.empty());
                        case 1:
                            return installedCardRepository.deleteAllRevealedCardByMonth(roomId, month)
                                    .thenMany(Flux.fromIterable(cardStack).concatWith(Flux.just(card)));
                        case 2:

                        case 3:
                            // TODO: size 2, 3인 경우 처리
                            return Flux.empty();
                        default:
                            return Flux.empty();
                    }
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
