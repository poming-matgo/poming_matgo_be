package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.domain.*;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.card.CardType;
import com.pomingmatgo.gameservice.domain.card.SpecialType;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.calculatescore.ScoreCalculator;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.*;


@Service
@RequiredArgsConstructor
public class GameService {
    private final InstalledCardRepository installedCardRepository;
    private final GameStateRepository gameStateRepository;
    private final AcquiredCardRepository acquiredCardRepository;
    private final ScoreCalculator scoreCalculator;

    public Mono<GameState> setGameInProgress(GameState gameState) {
        GameState newState = gameState.markInProgress();
        return gameStateRepository.save(newState)
                .thenReturn(newState);
    }

    public Mono<GameState> calculateAndApplyScores(long roomId, GameState gameState) {
        Mono<List<Card>> player1Card = acquiredCardRepository.getAllCards(roomId, 1);
        Mono<List<Card>> player2Card = acquiredCardRepository.getAllCards(roomId, 2);
        return Mono.zip(player1Card, player2Card)
                .flatMap(tuple -> {
                    List<Card> player1Cards = tuple.getT1();
                    List<Card> player2Cards = tuple.getT2();

                    int player1Score = scoreCalculator.calculateTotalScore(player1Cards);
                    int player2Score = scoreCalculator.calculateTotalScore(player2Cards);

                    GameState newState = gameState.updatePlayerState(Player.PLAYER_1, ps -> ps.toBuilder().score(player1Score).build())
                            .updatePlayerState(Player.PLAYER_2, ps -> ps.toBuilder().score(player2Score).build());
                    return gameStateRepository.save(newState).thenReturn(newState);
                });
    }

    public Mono<Void> acquireCards(long roomId, Player player, List<Card> acquiredCards) {
        return acquiredCardRepository.addCards(roomId, player.getNumber(), acquiredCards).then();
    }

    public Mono<Void> loseCard(long roomId, Player player, Card lostCard) {
        return acquiredCardRepository.removeCard(roomId, player.getNumber(), lostCard).then();
    }

    public Mono<Card> getTopCard(long roomId) {
        return installedCardRepository.getTopCard(roomId);
    }


    public Mono<Card> submitCardEvent(long roomId, Player player, int cardIndex) {
        return installedCardRepository.getPlayerCards(roomId, player)
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
                .flatMap(cardStack -> {
                    boolean isPpeokCondition = (cardStack.size() == 1);

                    if (isPpeokCondition) {
                        return processPpeok(roomId, submittedCard, turnedCard);
                    } else {
                        return processCardAcquisition(gameState, submittedCard, turnedCard, cardStack);
                    }
                });
    }

    private Mono<ProcessCardResult> processCardAcquisition(GameState gameState, Card submittedCard, Card turnedCard, List<Card> cardStack) {
        long roomId = gameState.getRoomId();
        int month = turnedCard.getMonth();

        List<Card> acquiredCards = Stream.concat(
                Stream.of(turnedCard, submittedCard),
                cardStack.stream()
        ).collect(Collectors.toList());

        Mono<Card> moveCardMono = determineCardToMove(
                gameState.getOtherPlayer(),
                roomId
        );

        return installedCardRepository.deleteAllRevealedCardByMonth(roomId, month)
                .then(moveCardMono)
                .map(movedCard -> {
                    acquiredCards.add(movedCard);
                    return cardStack.size() == 1
                            ? ProcessCardResult.jjok(acquiredCards, movedCard)
                            : ProcessCardResult.ttadak(acquiredCards, movedCard);
                });
    }

    private Mono<ProcessCardResult> processPpeok(long roomId, Card submittedCard, Card turnedCard) {
        return installedCardRepository.saveRevealedCard(List.of(turnedCard, submittedCard), roomId)
                .then(Mono.just(ProcessCardResult.ppeok(Collections.emptyList())));
    }

    private Mono<ProcessCardResult> handleDifferentMonthCards(GameState gameState, Card submittedCard, Card turnedCard) {
        return processCardByMonth(gameState, submittedCard, turnedCard)
                .flatMap(submittedResult -> {
                    if (submittedResult.isChoiceRequired()) {
                        return Mono.just(submittedResult);
                    }
                    return processCardByMonth(gameState, turnedCard, null)
                            .map(submittedResult::merge);
                });
    }

    private Mono<ProcessCardResult> processCardByMonth(GameState gameState, Card card, Card nextCard) {
        int month = card.getMonth();
        long roomId = gameState.getRoomId();

        return installedCardRepository.getRevealedCardByMonth(roomId, month)
                .flatMap(cardStack -> switch (cardStack.size()) {
                    case 0 -> handleZeroCardsOnFloor(card, roomId);
                    case 1 -> handleOneCardOnFloor(gameState, card, cardStack);
                    case 2 -> handleTwoCardsOnFloor(gameState, card, cardStack, nextCard);
                    case 3 -> handleThreeCardsOnFloor(gameState, card, cardStack);
                    default -> Mono.just(ProcessCardResult.immediate(Collections.emptyList()));
                });
    }

    private Mono<ProcessCardResult> handleZeroCardsOnFloor(Card card, long roomId) {
        return installedCardRepository.saveRevealedCard(List.of(card), roomId)
                .then(Mono.just(ProcessCardResult.immediate(Collections.emptyList())));
    }

    private Mono<List<Card>> acquireAndClearFloorCards(long roomId, int month, Card submittedCard, List<Card> cardStack) {
        List<Card> acquiredCards = new ArrayList<>(cardStack);
        acquiredCards.add(submittedCard);
        return installedCardRepository.deleteAllRevealedCardByMonth(roomId, month)
                .thenReturn(acquiredCards);
    }

    private Mono<ProcessCardResult> handleOneCardOnFloor(GameState gameState, Card submittedCard, List<Card> cardStack) {
        return acquireAndClearFloorCards(gameState.getRoomId(), submittedCard.getMonth(), submittedCard, cardStack)
                .map(ProcessCardResult::immediate);
    }

    private Mono<ProcessCardResult> handleThreeCardsOnFloor(GameState gameState, Card submittedCard, List<Card> cardStack) {
        Mono<List<Card>> acquiredCardsMono = acquireAndClearFloorCards(
                gameState.getRoomId(),
                submittedCard.getMonth(),
                submittedCard,
                cardStack
        );

        Mono<Card> moveCardMono = determineCardToMove(
                gameState.getOtherPlayer(),
                gameState.getRoomId()
        );

        return acquiredCardsMono.zipWith(moveCardMono)
                .map(tuple -> {
                    List<Card> acquiredCards = tuple.getT1();
                    Card movedCard = tuple.getT2();

                    acquiredCards.add(movedCard);
                    return ProcessCardResult.claimOpponentPi(acquiredCards, movedCard);
                });
    }

    private Mono<ProcessCardResult> handleTwoCardsOnFloor(GameState gameState, Card submittedCard, List<Card> selectableCards, Card turnedCard) {
        ChoiceInfo choiceInfo = ChoiceInfo.builder()
                .playerNumToChoose(gameState.getCurrentPlayer())
                .submittedCard(submittedCard)
                .selectableCards(selectableCards)
                .turnedCard(turnedCard)
                .build();

        GameState newGameState = gameState.toBuilder()
                .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                .choiceInfo(choiceInfo)
                .build();

        return gameStateRepository.save(newGameState)
                .thenReturn(ProcessCardResult.choicePending(selectableCards));
    }


    public Mono<ProcessCardResult> selectFloorCard(GameState gameState, Player player, RequestEvent<NormalSubmitReq> event) {
        int cardIndex = event.getData().cardIndex();
        validateFloorCardSelection(gameState, player, cardIndex);

        ChoiceInfo choiceInfo = gameState.getChoiceInfo();
        Card chosenFloorCard = choiceInfo.getSelectableCards().get(cardIndex);
        Card submittedCard = choiceInfo.getSubmittedCard();
        Card turnedCard = choiceInfo.getTurnedCard();

        ProcessCardResult baseResult = ProcessCardResult.immediate(List.of(chosenFloorCard, submittedCard));

        Mono<ProcessCardResult> turnResultMono;
        if (turnedCard == null) {
            turnResultMono = Mono.just(baseResult);
        } else {
            turnResultMono = processCardByMonth(gameState, turnedCard, null)
                    .map(baseResult::merge);
        }

        return turnResultMono
                .delayUntil(result ->
                        installedCardRepository.deleteRevealedCard(gameState.getRoomId(), chosenFloorCard)
                                .then(installedCardRepository.deleteRevealedCard(gameState.getRoomId(), submittedCard))
                )
                .flatMap(turnResult -> {
                    if (turnResult.isChoiceRequired()) {
                        return Mono.just(turnResult);
                    }
                    return finalizeTurn(gameState, choiceInfo.getPrevCards(), turnResult.getAcquiredCards());
                });
    }

    private void validateFloorCardSelection(GameState gameState, Player player, int cardIndex) {
        if (gameState.getPhase() != GamePhase.AWAITING_FLOOR_CARD_CHOICE) {
            throw new WebSocketBusinessException(NOT_EXIST_FLOOR_CARD);
        }
        ChoiceInfo choiceInfo = gameState.getChoiceInfo();
        if (choiceInfo.getPlayerNumToChoose() != player) {
            throw new WebSocketBusinessException(NOT_YOUR_TURN);
        }
        List<Card> selectableCards = choiceInfo.getSelectableCards();
        if (cardIndex < 0 || cardIndex >= selectableCards.size()) {
            throw new WebSocketBusinessException(INVALID_CARD);
        }
    }

    private Mono<ProcessCardResult> finalizeTurn(GameState gameState, List<Card> prevCards, List<Card> newCards) {
        List<Card> nonNullPrevCards = Optional.ofNullable(prevCards).orElse(Collections.emptyList());
        List<Card> finalAcquiredCards = new ArrayList<>(nonNullPrevCards);
        finalAcquiredCards.addAll(newCards);

        GameState newGameState = gameState.toBuilder()
                .phase(GamePhase.IN_PROGRESS)
                .choiceInfo(null)
                .build();

        return gameStateRepository.save(newGameState)
                .thenReturn(ProcessCardResult.immediate(finalAcquiredCards));
    }

    public Mono<Card> determineCardToMove(Player fromPlayer, long roomId) {
        return acquiredCardRepository.getAllCards(roomId, fromPlayer.getNumber())
                .flatMap(playerCards ->
                        Mono.justOrEmpty(findMovablePiCard(playerCards))
                );
    }

    private Optional<Card> findMovablePiCard(List<Card> playerCards) {
        return playerCards.stream()
                .filter(c -> c.getType() == CardType.PI)
                .min(Comparator.comparing(c -> c.getSpecialType() == SpecialType.SSANG_PI));
    }

    public Mono<GameState> setNextTurn(GameState gameState) {
        GameState.GameStateBuilder builder = gameState.toBuilder()
                .currentTurn((gameState.getCurrentTurn() == 1 ? 2 : 1))
                .phase(GamePhase.IN_PROGRESS)
                .choiceInfo(null);

        if (gameState.getCurrentTurn() == 2) {
            builder.round(gameState.getRound() + 1); // todo: 마지막 라운드는 향후 처리 예정
        }

        GameState newGameState = builder.build();

        return Mono.just(newGameState);
    }

    public Mono<GameState> executeGoStop(GameState gameState, Player player) {
        GameState newState = gameState.updatePlayerState(player, ps -> ps.toBuilder()
                .go(ps.getGo() + 1)
                .goScore(ps.getScore())
                .build()
        );
        return Mono.just(newState);
    }
}