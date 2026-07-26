package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.*;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.CardMatchEngine.FloorEffect;
import com.pomingmatgo.gameservice.domain.service.matgo.CardMatchEngine.MatchOutcome;
import com.pomingmatgo.gameservice.domain.score.ScoreBreakdown;
import com.pomingmatgo.gameservice.domain.service.matgo.calculatescore.ScoreCalculator;
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
    private final AcquiredCardRepository acquiredCardRepository;
    private final ScoreCalculator scoreCalculator;
    private final RoomCleanupService roomCleanupService;
    private final CardMatchEngine cardMatchEngine;

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
                    ScoreBreakdown player1Score = scoreCalculator.calculate(tuple.getT1());
                    ScoreBreakdown player2Score = scoreCalculator.calculate(tuple.getT2());

                    GameState newState = gameState.updatePlayerState(Player.PLAYER_1, ps -> applyScore(ps, player1Score))
                            .updatePlayerState(Player.PLAYER_2, ps -> applyScore(ps, player2Score));
                    return gameStateRepository.save(newState).thenReturn(newState);
                });
    }

    // score와 breakdown은 항상 함께 갱신한다 (PlayerState.breakdown 불변식)
    private PlayerState applyScore(PlayerState playerState, ScoreBreakdown breakdown) {
        return playerState.toBuilder()
                .score(breakdown.total())
                .breakdown(breakdown)
                .build();
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
        long roomId = gameState.getRoomId();
        Mono<List<Card>> submittedStack = installedCardRepository.getRevealedCardByMonth(roomId, submittedCard.getMonth());
        Mono<List<Card>> turnedStack = turnedCard.hasSameMonthAs(submittedCard)
                ? Mono.just(Collections.emptyList())
                : installedCardRepository.getRevealedCardByMonth(roomId, turnedCard.getMonth());

        return Mono.zip(submittedStack, turnedStack, opponentAcquiredCards(gameState))
                .map(t -> cardMatchEngine.decideSubmit(
                        gameState.getCurrentPlayer(), submittedCard, turnedCard, t.getT1(), t.getT2(), t.getT3()))
                .flatMap(outcome -> applyOutcome(gameState, outcome));
    }

    public Mono<ProcessCardResult> selectFloorCard(GameState gameState, Player player, int cardIndex) {
        validateFloorCardSelection(gameState, player, cardIndex);

        ChoiceInfo choiceInfo = gameState.getChoiceInfo();
        Card chosenFloorCard = choiceInfo.getSelectableCards().get(cardIndex);
        Card turnedCard = choiceInfo.getTurnedCard();
        long roomId = gameState.getRoomId();

        Mono<List<Card>> turnedStack = turnedCard == null
                ? Mono.just(Collections.emptyList())
                : installedCardRepository.getRevealedCardByMonth(roomId, turnedCard.getMonth());

        return Mono.zip(turnedStack, opponentAcquiredCards(gameState))
                .map(t -> cardMatchEngine.decideFloorSelection(
                        gameState.getCurrentPlayer(), choiceInfo, chosenFloorCard, t.getT1(), t.getT2()))
                .flatMap(outcome -> applyOutcome(gameState, outcome));
    }

    private Mono<List<Card>> opponentAcquiredCards(GameState gameState) {
        return acquiredCardRepository.getAllCards(gameState.getRoomId(), gameState.getOtherPlayer().getNumber());
    }

    private Mono<ProcessCardResult> applyOutcome(GameState gameState, MatchOutcome outcome) {
        return applyFloorEffects(gameState.getRoomId(), outcome.effects())
                .then(saveResultingPhase(gameState, outcome))
                .thenReturn(outcome.result());
    }

    private Mono<Void> applyFloorEffects(long roomId, List<FloorEffect> effects) {
        return Flux.fromIterable(effects)
                .concatMap(effect -> switch (effect) {
                    case FloorEffect.Place place -> installedCardRepository.saveRevealedCard(place.cards(), roomId);
                    case FloorEffect.ClearMonth clear -> installedCardRepository.deleteAllRevealedCardByMonth(roomId, clear.month());
                    case FloorEffect.Remove remove -> installedCardRepository.deleteRevealedCard(roomId, remove.card());
                })
                .then();
    }

    // phase가 바뀔 때만 저장한다 — 선택 없이 끝난 정상 제출의 점수 저장은 settleTurn 담당
    private Mono<Void> saveResultingPhase(GameState gameState, MatchOutcome outcome) {
        if (outcome.pendingChoice() != null) {
            return gameStateRepository.save(gameState.toBuilder()
                    .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                    .choiceInfo(outcome.pendingChoice())
                    .build()).then();
        }
        if (gameState.getPhase() == GamePhase.AWAITING_FLOOR_CARD_CHOICE) {
            return gameStateRepository.save(gameState.toBuilder()
                    .phase(GamePhase.IN_PROGRESS)
                    .choiceInfo(null)
                    .build()).then();
        }
        return Mono.empty();
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

    /** phase를 저장해야 선택 요청 검증과 자동플레이 타이머(TurnStep)가 같은 상태를 본다 */
    public Mono<GameState> enterGoStopChoice(GameState gameState) {
        GameState newState = gameState.toBuilder()
                .phase(GamePhase.AWAITING_GO_STOP_CHOICE)
                .build();
        return gameStateRepository.save(newState)
                .thenReturn(newState);
    }

    public Mono<GameState> applyGo(GameState gameState, Player player) {
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
