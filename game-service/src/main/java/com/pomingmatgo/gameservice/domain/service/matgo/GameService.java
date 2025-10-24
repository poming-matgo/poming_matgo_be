package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.ScoreCardRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALUD_CARD;


@Service
@RequiredArgsConstructor
public class GameService {
    private final InstalledCardRepository installedCardRepository;
    private final ScoreCardRepository scoreCardRepository;
    public Mono<Boolean> isConfusedPlayer(long roomId, Player player) {
        Flux<Card> cardFlux = installedCardRepository.getPlayerCards(roomId, player);

        return cardFlux
                .groupBy(Card::getMonth)
                .flatMap(group -> group.count().map(count -> count == 4))
                .any(Boolean::booleanValue);
    }

    public Mono<Card> getTopCard(long roomId) {
        return installedCardRepository.getTopCard(roomId);
    }

    public Mono<Card> submitCardEvent(long roomId, Player player, RequestEvent<NormalSubmitReq> event) {
        int cardIndex = event.getData().getCardIndex();

        return installedCardRepository.getPlayerCards(roomId, player)
                .collectList()
                .flatMap(playerCards -> {
                    if (cardIndex < 0 || cardIndex >= playerCards.size()) {
                        return Mono.error(new WebSocketBusinessException(INVALUD_CARD));
                    }
                    List<Card> mutablePlayerCards = new ArrayList<>(playerCards);
                    Card submittedCard = mutablePlayerCards.remove(cardIndex);
                    return installedCardRepository.updatePlayerCards(roomId, player, mutablePlayerCards)
                            .thenReturn(submittedCard);
                });
    }


    public Mono<List<Card>> submitCard(long roomId, Card submittedCard, Card turnedCard) {
        if (turnedCard.hasSameMonthAs(submittedCard)) {
            return handleSameMonthCards(roomId, submittedCard, turnedCard);
        } else {
            return handleDifferentMonthCards(roomId, submittedCard, turnedCard);
        }
    }

    private Mono<List<Card>> handleSameMonthCards(long roomId, Card submittedCard, Card turnedCard) {
        int month = turnedCard.getMonth();
        return installedCardRepository.getRevealedCardByMonth(roomId, month)
                .collectList()
                .flatMap(cardStack -> {
                    if (cardStack.size() != 1) {
                        List<Card> acquiredCards = new ArrayList<>();
                        acquiredCards.add(turnedCard);
                        acquiredCards.add(submittedCard);
                        acquiredCards.addAll(cardStack);

                        return installedCardRepository.deleteAllRevealedCardByMonth(roomId, month)
                                .then(Mono.just(acquiredCards));
                        //todo: 다른 사람 카드 가져오는 로직 추가
                    } else {
                        //뻑
                        return installedCardRepository.saveRevealedCard(List.of(turnedCard, submittedCard), roomId)
                                .then(Mono.just(Collections.emptyList()));
                    }
                });
    }

    private Mono<List<Card>> handleDifferentMonthCards(long roomId, Card submittedCard, Card turnedCard) {
        Mono<List<Card>> submittedResult = processCardByMonth(roomId, submittedCard);
        Mono<List<Card>> turnedResult = processCardByMonth(roomId, turnedCard);

        return Mono.zip(submittedResult, turnedResult)
                .map(tuple -> {
                    List<Card> combinedList = new ArrayList<>(tuple.getT1());
                    combinedList.addAll(tuple.getT2());
                    return combinedList;
                });
    }

    private Mono<List<Card>> processCardByMonth(long roomId, Card card) {
        int month = card.getMonth();
        return installedCardRepository.getRevealedCardByMonth(roomId, month)
                .collectList()
                .flatMap(cardStack -> {
                    int size = cardStack.size();
                    switch (size) {
                        case 0:
                            return installedCardRepository.saveRevealedCard(List.of(card), roomId)
                                    .then(Mono.just(Collections.emptyList()));
                        case 1:
                            List<Card> acquiredCards = new ArrayList<>(cardStack);
                            acquiredCards.add(card);
                            return installedCardRepository.deleteAllRevealedCardByMonth(roomId, month)
                                    .then(Mono.just(acquiredCards));
                        case 2:
                        case 3:
                            // TODO: size 2, 3인 경우 처리
                            return Mono.just(Collections.emptyList());
                        default:
                            return Mono.just(Collections.emptyList());
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
