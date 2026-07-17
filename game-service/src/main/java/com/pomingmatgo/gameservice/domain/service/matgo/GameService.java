package com.pomingmatgo.gameservice.domain.service.matgo;

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
    private final RoomCleanupService roomCleanupService;

    public Mono<GameState> findGameState(long roomId) {
        return gameStateRepository.findById(roomId);
    }

    public Mono<GameState> saveState(GameState gameState) {
        return gameStateRepository.save(gameState)
                .thenReturn(gameState);
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

    public Mono<Card> drawTopCard(long roomId) {
        return installedCardRepository.drawTopCard(roomId);
    }

    /** 제출할 카드를 손패에서 꺼낸다(손패에서 제거 후 반환) */
    public Mono<Card> takeCardFromHand(long roomId, Player player, int cardIndex) {
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
                .then(moveCardMono
                        .map(movedCard -> {
                            acquiredCards.add(movedCard);
                            // 같은 월 스택이 비어 있었다면 쪽(낸 카드+뒤집은 카드만 매치), 2장이었다면 따닥.
                            // 1장(뻑)은 handleSameMonthCards에서 걸러져 여기 도달하지 않는다
                            return cardStack.isEmpty()
                                    ? ProcessCardResult.jjok(acquiredCards, movedCard)
                                    : ProcessCardResult.ttadak(acquiredCards, movedCard);
                        })
                        .switchIfEmpty(Mono.fromSupplier(() -> ProcessCardResult.immediate(acquiredCards)))
                );
    }

    private Mono<ProcessCardResult> processPpeok(long roomId, Card submittedCard, Card turnedCard) {
        return installedCardRepository.saveRevealedCard(List.of(turnedCard, submittedCard), roomId)
                .then(Mono.just(ProcessCardResult.ppeok(Collections.emptyList())));
    }

    private Mono<ProcessCardResult> handleDifferentMonthCards(GameState gameState, Card submittedCard, Card turnedCard) {
        return processCardByMonth(gameState, submittedCard, turnedCard, null)
                .flatMap(submittedResult -> {
                    if (submittedResult.isChoiceRequired()) {
                        return Mono.just(submittedResult);
                    }
                    // 뒤집은 카드가 선택 대기를 만들면 낸 카드의 획득분이 result 흐름에서 끊기므로
                    // choiceInfo.prev*로 이월한다 (finalizeTurn에서 복원)
                    return processCardByMonth(gameState, turnedCard, null, submittedResult)
                            .map(submittedResult::merge);
                });
    }

    /** @param prevResult 이 턴에서 앞서 확정된 획득/피 뺏기. 이 카드가 선택 대기를 만들 때 choiceInfo로 이월된다 (없으면 null) */
    private Mono<ProcessCardResult> processCardByMonth(GameState gameState, Card card, Card nextCard, ProcessCardResult prevResult) {
        int month = card.getMonth();
        long roomId = gameState.getRoomId();

        return installedCardRepository.getRevealedCardByMonth(roomId, month)
                .flatMap(cardStack -> switch (cardStack.size()) {
                    case 0 -> handleZeroCardsOnFloor(card, roomId);
                    case 1 -> handleOneCardOnFloor(gameState, card, cardStack);
                    case 2 -> handleTwoCardsOnFloor(gameState, card, cardStack, nextCard, prevResult);
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

        return acquiredCardsMono.flatMap(acquiredCards ->
                moveCardMono
                        .map(movedCard -> {
                            acquiredCards.add(movedCard);
                            return ProcessCardResult.claimOpponentPi(acquiredCards, movedCard);
                        })
                        .switchIfEmpty(Mono.fromSupplier(() -> ProcessCardResult.immediate(acquiredCards)))
        );
    }

    private Mono<ProcessCardResult> handleTwoCardsOnFloor(GameState gameState, Card submittedCard, List<Card> selectableCards, Card turnedCard, ProcessCardResult prevResult) {
        ChoiceInfo choiceInfo = ChoiceInfo.builder()
                .playerNumToChoose(gameState.getCurrentPlayer())
                .submittedCard(submittedCard)
                .selectableCards(selectableCards)
                .turnedCard(turnedCard)
                // 앞서 확정된 획득/피 뺏기를 이월 — 선택 완료 시 finalizeTurn이 최종 결과에 복원한다
                .prevCards(prevResult != null ? prevResult.getAcquiredCards() : null)
                .prevMoveCards(prevResult != null ? prevResult.getMoveCards() : null)
                .build();

        GameState newGameState = gameState.toBuilder()
                .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                .choiceInfo(choiceInfo)
                .build();

        return gameStateRepository.save(newGameState)
                .thenReturn(ProcessCardResult.choicePending(selectableCards));
    }


    public Mono<ProcessCardResult> selectFloorCard(GameState gameState, Player player, int cardIndex) {
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
            // 뒤집은 카드가 또 선택을 요구하면 새 choiceInfo가 기존 이월분 + 이번 선택 획득분을 물려받아야 한다
            ProcessCardResult carryover = restorePrevResult(choiceInfo).merge(baseResult);
            turnResultMono = processCardByMonth(gameState, turnedCard, null, carryover)
                    .map(baseResult::merge);
        }

        return turnResultMono
                // 낸 카드는 선택 대기 진입 시 바닥에 저장되지 않고 choiceInfo로만 이월되므로 바닥 삭제 대상은 선택된 카드뿐
                .delayUntil(result ->
                        installedCardRepository.deleteRevealedCard(gameState.getRoomId(), chosenFloorCard))
                .flatMap(turnResult -> {
                    if (turnResult.isChoiceRequired()) {
                        return Mono.just(turnResult);
                    }
                    return finalizeTurn(gameState, choiceInfo, turnResult);
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

    /** 선택 대기 전에 확정돼 choiceInfo.prev*로 이월된 획득/피 뺏기를 최종 결과에 복원하고 선택 대기를 해제한다 */
    private Mono<ProcessCardResult> finalizeTurn(GameState gameState, ChoiceInfo choiceInfo, ProcessCardResult turnResult) {
        ProcessCardResult finalResult = restorePrevResult(choiceInfo).merge(turnResult);

        GameState newGameState = gameState.toBuilder()
                .phase(GamePhase.IN_PROGRESS)
                .choiceInfo(null)
                .build();

        return gameStateRepository.save(newGameState)
                .thenReturn(finalResult);
    }

    /** choiceInfo에 이월된 prev* 목록을 ProcessCardResult로 되살린다 (merge로 이후 결과와 합치기 위함) */
    private ProcessCardResult restorePrevResult(ChoiceInfo choiceInfo) {
        return ProcessCardResult.builder()
                .acquiredCards(new ArrayList<>(nullSafe(choiceInfo.getPrevCards())))
                .moveCards(new ArrayList<>(nullSafe(choiceInfo.getPrevMoveCards())))
                .build();
    }

    private static List<Card> nullSafe(List<Card> cards) {
        return cards != null ? cards : Collections.emptyList();
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

    /** 고/스톱 선택 대기 진입 — phase를 저장해 선택 요청 검증과 자동플레이 타이머(TurnStep)가 같은 상태를 공유하게 한다 */
    public Mono<GameState> enterGoStopChoice(GameState gameState) {
        GameState newState = gameState.toBuilder()
                .phase(GamePhase.AWAITING_GO_STOP_CHOICE)
                .build();
        return gameStateRepository.save(newState)
                .thenReturn(newState);
    }

    public Mono<GameState> executeGoStop(GameState gameState, Player player) {
        GameState newState = gameState.updatePlayerState(player, ps -> ps.toBuilder()
                .go(ps.getGo() + 1)
                .goScore(ps.getScore())
                .build()
        );
        return Mono.just(newState);
    }

    public Mono<GameState> gameOver(GameState gameState) {
        GameState newState = gameState.toBuilder()
                .phase(GamePhase.END)
                .build();
        long roomId = gameState.getRoomId();
        GameState initState = GameState.createEmptyRoom(roomId);

        return roomCleanupService.cleanupRoomData(roomId)
                .then(Mono.defer(() -> gameStateRepository.create(initState)))
                .thenReturn(newState);
    }
}