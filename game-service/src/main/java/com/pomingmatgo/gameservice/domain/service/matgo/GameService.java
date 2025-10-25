package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.domain.*;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.*;


@Service
@RequiredArgsConstructor
public class GameService {
    private final InstalledCardRepository installedCardRepository;
    private final GameStateRepository gameStateRepository;
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
                        return Mono.error(new WebSocketBusinessException(INVALID_CARD));
                    }
                    List<Card> mutablePlayerCards = new ArrayList<>(playerCards);
                    Card submittedCard = mutablePlayerCards.remove(cardIndex);
                    return installedCardRepository.updatePlayerCards(roomId, player, mutablePlayerCards)
                            .thenReturn(submittedCard);
                });
    }


    public Mono<ProcessCardResult> submitCard(GameState gameState, Card submittedCard, Card turnedCard) {
        if (turnedCard.hasSameMonthAs(submittedCard)) {
            return handleSameMonthCards(gameState, submittedCard, turnedCard);
        } else {
            return handleDifferentMonthCards(gameState, submittedCard, turnedCard);
        }
    }

    private Mono<ProcessCardResult> handleSameMonthCards(GameState gameState, Card submittedCard, Card turnedCard) {
        int month = turnedCard.getMonth();
        long roomId = gameState.getRoomId();
        return installedCardRepository.getRevealedCardByMonth(roomId, month)
                .collectList()
                .flatMap(cardStack -> {
                    if (cardStack.size() != 1) {
                        List<Card> acquiredCards = new ArrayList<>();
                        acquiredCards.add(turnedCard);
                        acquiredCards.add(submittedCard);
                        acquiredCards.addAll(cardStack);

                        return installedCardRepository.deleteAllRevealedCardByMonth(roomId, month)
                                .then(Mono.just(ProcessCardResult.immediate(acquiredCards)));
                        //todo: 다른 사람 카드 가져오는 로직 추가
                    } else {
                        //뻑
                        return installedCardRepository.saveRevealedCard(List.of(turnedCard, submittedCard), roomId)
                                .then(Mono.just(ProcessCardResult.immediate(Collections.emptyList())));
                    }
                });
    }

    private Mono<ProcessCardResult> handleDifferentMonthCards(GameState gameState, Card submittedCard, Card turnedCard) {
        return processCardByMonth(gameState, submittedCard)
                .flatMap(submittedResult -> {
                    if (submittedResult.isChoiceRequired()) {
                        return Mono.just(submittedResult);
                    }

                    return processCardByMonth(gameState, turnedCard)
                            .map(turnedResult -> {
                                if (turnedResult.isChoiceRequired()) {
                                    return turnedResult;
                                }

                                List<Card> combinedList = new ArrayList<>(submittedResult.getAcquiredCards());
                                combinedList.addAll(turnedResult.getAcquiredCards());

                                return ProcessCardResult.immediate(combinedList);
                            });
                });
    }

    private Mono<ProcessCardResult> processCardByMonth(GameState gameState, Card card) {
        int month = card.getMonth();
        long roomId = gameState.getRoomId();
        return installedCardRepository.getRevealedCardByMonth(roomId, month)
                .collectList()
                .flatMap(cardStack -> {
                    int size = cardStack.size();
                    switch (size) {
                        case 0:
                            return installedCardRepository.saveRevealedCard(List.of(card), roomId)
                                    .then(Mono.just(ProcessCardResult.immediate(Collections.emptyList())));
                        case 1:
                            List<Card> acquiredCards = new ArrayList<>(cardStack);
                            acquiredCards.add(card);
                            return installedCardRepository.deleteAllRevealedCardByMonth(roomId, month)
                                    .then(Mono.just(ProcessCardResult.immediate(acquiredCards)));
                        case 2:
                            ChoiceInfo choiceInfo = ChoiceInfo.builder()
                                    .playerNumToChoose(gameState.getCurrentPlayer())
                                    .submittedCard(card)
                                    .selectableCards(cardStack)
                                    .build();

                            GameState newGameState = gameState.toBuilder()
                                    .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                                    .choiceInfo(choiceInfo)
                                    .build();

                            return gameStateRepository.save(newGameState)
                                    .thenReturn(ProcessCardResult.choicePending(cardStack));

                        case 3:
                            // TODO: size 2, 3인 경우 처리
                            return Mono.just(ProcessCardResult.immediate(Collections.emptyList()));
                        default:
                            return Mono.just(ProcessCardResult.immediate(Collections.emptyList()));
                    }
                });
    }

    public Mono<List<Card>> selectFloorCard(GameState gameState, Player player, RequestEvent<NormalSubmitReq> event) {
        int cardIndex = event.getData().getCardIndex();
        ChoiceInfo choiceInfo = gameState.getChoiceInfo();
        if(gameState.getPhase() != GamePhase.AWAITING_FLOOR_CARD_CHOICE)
            return Mono.error(new WebSocketBusinessException(NOT_EXIST_FLOOR_CARD));
        if(choiceInfo.getPlayerNumToChoose()!=player)
            return Mono.error(new WebSocketBusinessException(NOT_YOUR_TURN));

        List<Card> selectableCards = choiceInfo.getSelectableCards();
        if (cardIndex < 0 || cardIndex >= selectableCards.size()) {
            return Mono.error(new WebSocketBusinessException(INVALID_CARD));
        }

        Card card = selectableCards.get(cardIndex);

        GameState.GameStateBuilder builder = gameState.toBuilder();
        builder.phase(GamePhase.IN_PROGRESS)
                .choiceInfo(null);
        GameState newGameState = builder.build();

        return installedCardRepository.deleteRevealedCard(gameState.getRoomId(), card)
                .then(installedCardRepository.deleteRevealedCard(gameState.getRoomId(), choiceInfo.getSubmittedCard()))
                .then(gameStateRepository.save(newGameState))
                .thenReturn(List.of(card, choiceInfo.getSubmittedCard()));
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
