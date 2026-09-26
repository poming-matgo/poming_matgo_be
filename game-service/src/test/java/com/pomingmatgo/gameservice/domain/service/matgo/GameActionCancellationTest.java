package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.ChoiceInfo;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InMemoryInstalledCardRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.scheduler.TurnScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_CARD;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.ArgumentMatchers.*;

@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@ActiveProfiles("in-memory")
@DisplayName("수락한 카드 변경의 호출자 취소 분리")
class GameActionCancellationTest {
    private static final long ROOM_ID = 940_001L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private static final List<Card> INITIAL_CARDS = List.of(Card.JAN_3, Card.FEB_3, Card.MAR_1, Card.APR_1);

    @Autowired GamePlayService gamePlayService;
    @Autowired TurnFlowService turnFlowService;
    @SpyBean GameStateRepository gameStateRepository;
    @Autowired AcquiredCardRepository acquiredCardRepository;
    @Autowired RoomCleanupService roomCleanupService;
    @SpyBean InMemoryInstalledCardRepository installedCardRepository;

    private final Sinks.Empty<Void> gate = Sinks.empty();
    private final AtomicInteger waiting = new AtomicInteger();
    private final AtomicInteger cancelled = new AtomicInteger();
    private final AtomicInteger succeeded = new AtomicInteger();
    private final TurnScheduler scheduler = mock(TurnScheduler.class);

    @BeforeEach
    void setUp() {
        assertTrue(AopUtils.isAopProxy(gamePlayService));
        gameStateRepository.create(GameState.builder()
                .roomId(ROOM_ID).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS).build()).block(TIMEOUT);
        installedCardRepository.savePlayerCards(List.of(Card.JAN_3), ROOM_ID, Player.PLAYER_1).block(TIMEOUT);
        installedCardRepository.savePlayerCards(List.of(Card.FEB_3), ROOM_ID, Player.PLAYER_2).block(TIMEOUT);
        installedCardRepository.saveHiddenCard(List.of(Card.MAR_1, Card.APR_1), ROOM_ID).block(TIMEOUT);
        assertEquals(INITIAL_CARDS, storedCards());
    }

    @AfterEach
    void cleanup() {
        gate.tryEmitEmpty();
        roomCleanupService.cleanupRoomData(ROOM_ID).block(TIMEOUT);
    }

    @ParameterizedTest(name = "덱 제거 후 대기={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("손패 또는 손패·덱 제거 후 호출자가 취소해도 카드와 다음 턴 저장을 완료한다")
    void cancellationFinishesAcceptedActionBeforeReleasingLock(boolean afterDraw) {
        pauseDraw(afterDraw);

        StepVerifier.create(turnFlowService.processNormalSubmit(ROOM_ID, Player.PLAYER_1, 0,
                        succeeded::incrementAndGet, scheduler))
                .then(() -> assertPaused(afterDraw))
                .thenCancel()
                .verify(TIMEOUT);

        assertEquals(0, cancelled.get());
        assertEquals(0, succeeded.get());
        assertOriginalTurn();
        assertEquals(remainingCards(afterDraw), storedCards());
        StepVerifier.create(submit())
                .expectErrorSatisfies(error -> assertCode(error, TRY_AGAIN))
                .verify(TIMEOUT);
        assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
        assertEquals(INITIAL_CARDS, storedCards());
        assertEquals(2, gameStateRepository.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
        assertEquals(1, succeeded.get());
        verifyNextTimer(GamePhase.IN_PROGRESS, 2);

        StepVerifier.create(submit())
                .expectErrorSatisfies(error -> assertCode(error, WebSocketErrorCode.NOT_YOUR_TURN))
                .verify(TIMEOUT);
        assertEquals(INITIAL_CARDS, storedCards());
    }

    @Test
    void invalidHandIndexDoesNotConsumeDeckOrBlockRoom() {
        StepVerifier.create(gamePlayService.executeNormalSubmit(ROOM_ID, Player.PLAYER_1, 9, null))
                .expectErrorSatisfies(error -> assertCode(error, INVALID_CARD)).verify(TIMEOUT);
        assertOriginalTurn();
        assertEquals(INITIAL_CARDS, storedCards());
        assertNotNull(submit().block(TIMEOUT));
    }

    @ParameterizedTest(name = "GO={0}")
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void goStopSaveContinuesAfterCallerCancellation(boolean go) {
        GameState choice = gameStateRepository.findById(ROOM_ID).block(TIMEOUT).toBuilder()
                .phase(GamePhase.AWAITING_GO_STOP_CHOICE).build();
        gameStateRepository.save(choice).block(TIMEOUT);
        doAnswer(invocation -> {
            Mono<Long> save = (Mono<Long>) invocation.callRealMethod();
            return gate.asMono().doOnCancel(cancelled::incrementAndGet).then(save);
        }).when(gameStateRepository).save(any(GameState.class));

        StepVerifier.create(turnFlowService.processGoStopChoice(ROOM_ID, Player.PLAYER_1, go,
                        succeeded::incrementAndGet, scheduler))
                .then(() -> assertEquals(1, gate.currentSubscriberCount()))
                .thenCancel().verify(TIMEOUT);
        assertEquals(0, cancelled.get());
        assertEquals(choice, gameStateRepository.findById(ROOM_ID).block(TIMEOUT));
        gate.tryEmitEmpty();
        GameState saved = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        assertEquals(go ? GamePhase.IN_PROGRESS : GamePhase.END, saved.getPhase());
        assertEquals(go ? 2 : 1, saved.getCurrentTurn());
        assertEquals(go ? 1 : 0, saved.getPlayerState(Player.PLAYER_1).getGo());
        assertEquals(1, succeeded.get());
        if (go) verifyNextTimer(GamePhase.IN_PROGRESS, 2);
        else verify(scheduler, never()).scheduleAutoPlay(anyLong(), anyInt(), anyInt(), any(), anyLong(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void floorSelectionFinishesAcquisitionAndTurnAfterCallerCancellation() {
        ChoiceInfo choice = ChoiceInfo.builder().playerNumToChoose(Player.PLAYER_1)
                .submittedCard(Card.JAN_3).selectableCards(List.of(Card.JAN_1, Card.JAN_2)).build();
        gameStateRepository.save(gameStateRepository.findById(ROOM_ID).block(TIMEOUT).toBuilder()
                .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE).choiceInfo(choice).build()).block(TIMEOUT);
        installedCardRepository.updatePlayerCards(ROOM_ID, Player.PLAYER_1, List.of()).block(TIMEOUT);
        installedCardRepository.saveRevealedCard(List.of(Card.JAN_1, Card.JAN_2), ROOM_ID).block(TIMEOUT);
        doAnswer(invocation -> {
            Mono<Boolean> deletion = (Mono<Boolean>) invocation.callRealMethod();
            return deletion.delayUntil(ignored -> gate.asMono().doOnCancel(cancelled::incrementAndGet));
        }).when(installedCardRepository).deleteRevealedCard(ROOM_ID, Card.JAN_1);

        StepVerifier.create(turnFlowService.processFloorSelection(ROOM_ID, Player.PLAYER_1, 0,
                        succeeded::incrementAndGet, scheduler))
                .then(() -> assertEquals(1, gate.currentSubscriberCount()))
                .thenCancel().verify(TIMEOUT);
        assertEquals(0, cancelled.get());
        gate.tryEmitEmpty();
        GameState saved = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        assertEquals(GamePhase.IN_PROGRESS, saved.getPhase());
        assertEquals(2, saved.getCurrentTurn());
        assertNull(saved.getChoiceInfo());
        assertEquals(List.of(Card.JAN_1, Card.JAN_3), acquiredCardRepository.getAllCards(ROOM_ID, 1).block(TIMEOUT));
        assertEquals(List.of(Card.JAN_2), installedCardRepository.getAllRevealedCards(ROOM_ID).block(TIMEOUT));
        assertEquals(1, succeeded.get());
        verifyNextTimer(GamePhase.IN_PROGRESS, 2);
    }

    @Test
    @SuppressWarnings("unchecked")
    void cancellationBeforeHandValidationDoesNotMutateCards() {
        doAnswer(invocation -> {
            Mono<List<Card>> cards = (Mono<List<Card>>) invocation.callRealMethod();
            return cards.delayUntil(ignored -> gate.asMono().doOnCancel(cancelled::incrementAndGet));
        }).when(installedCardRepository).getPlayerCards(ROOM_ID, Player.PLAYER_1);

        StepVerifier.create(submit())
                .then(() -> assertEquals(1, gate.currentSubscriberCount()))
                .thenCancel().verify(TIMEOUT);
        assertEquals(1, cancelled.get());
        assertEquals(0, succeeded.get());
        gate.tryEmitEmpty();
        assertOriginalTurn();
        assertEquals(INITIAL_CARDS, storedCards());
        assertNotNull(submit().block(TIMEOUT));
    }

    @Test
    void acceptedFailureAfterCallerCancellationBlocksFurtherActionsUntilCleanup() {
        pauseDraw(true);
        StepVerifier.create(submit()).then(() -> assertPaused(true)).thenCancel().verify(TIMEOUT);
        assertEquals(Sinks.EmitResult.OK, gate.tryEmitError(new IllegalStateException("draw failed")));
        StepVerifier.create(submit()).expectErrorSatisfies(error -> assertCode(error, TRY_AGAIN)).verify(TIMEOUT);
        assertEquals(0, succeeded.get());
        roomCleanupService.cleanupRoomData(ROOM_ID).block(TIMEOUT);
        assertNull(gameStateRepository.findById(ROOM_ID).block(TIMEOUT));
        gameStateRepository.create(GameState.builder().roomId(ROOM_ID).leadingPlayer(1).currentTurn(1)
                .round(2).phase(GamePhase.AWAITING_GO_STOP_CHOICE).build()).block(TIMEOUT);
        assertEquals(GamePhase.END, gamePlayService.executeGoStop(ROOM_ID, Player.PLAYER_1, false, null)
                .block(TIMEOUT).getPhase());
    }

    @ParameterizedTest(name = "덱 제거 후 대기={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("같은 경계에서 취소 없이 재개하면 카드 소유권과 다음 턴 저장이 완료된다")
    void completionPreservesCardsAndAdvancesTurn(boolean afterDraw) {
        pauseDraw(afterDraw);

        StepVerifier.create(submit())
                .then(() -> {
                    assertPaused(afterDraw);
                    assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
                })
                .assertNext(result -> assertEquals(Player.PLAYER_2, result.updatedGameState().getCurrentPlayer()))
                .expectComplete()
                .verify(TIMEOUT);

        assertEquals(0, cancelled.get());
        assertEquals(1, succeeded.get());
        GameState state = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        assertNotNull(state);
        assertEquals(GamePhase.IN_PROGRESS, state.getPhase());
        assertEquals(1, state.getRound());
        assertEquals(2, state.getCurrentTurn());
        assertEquals(Player.PLAYER_2, state.getCurrentPlayer());
        assertEquals(List.of(), installedCardRepository.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        assertEquals(List.of(Card.JAN_3, Card.MAR_1), installedCardRepository.getAllRevealedCards(ROOM_ID).block(TIMEOUT));
        assertEquals(INITIAL_CARDS, storedCards(), "카드 총수뿐 아니라 각 카드의 중복·누락도 확인한다");
    }

    @SuppressWarnings("unchecked")
    private void pauseDraw(boolean afterDraw) {
        doAnswer(invocation -> {
            Mono<Card> realDraw = (Mono<Card>) invocation.callRealMethod();
            Mono<Void> pause = Mono.defer(() -> {
                waiting.incrementAndGet();
                return gate.asMono().doOnCancel(cancelled::incrementAndGet);
            });
            return afterDraw ? realDraw.delayUntil(card -> pause) : pause.then(realDraw);
        }).when(installedCardRepository).drawTopCard(ROOM_ID);
    }

    private Mono<TurnExecutionResult> submit() {
        return gamePlayService.executeNormalSubmit(ROOM_ID, Player.PLAYER_1, 0, succeeded::incrementAndGet);
    }

    private void verifyNextTimer(GamePhase phase, int turn) {
        verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(turn),
                eq(Player.PLAYER_2), anyLong(), eq(phase));
    }

    private void assertPaused(boolean afterDraw) {
        assertEquals(1, waiting.get(), "제어 Publisher 구독에 도달한 뒤 취소 또는 재개한다");
        assertEquals(0, succeeded.get());
        assertOriginalTurn();
        assertEquals(remainingCards(afterDraw), storedCards());
        StepVerifier.create(submit())
                .expectErrorSatisfies(error -> assertCode(error, TRY_AGAIN))
                .verify(TIMEOUT);
    }

    private void assertOriginalTurn() {
        GameState state = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        assertNotNull(state);
        assertEquals(GamePhase.IN_PROGRESS, state.getPhase());
        assertEquals(1, state.getRound());
        assertEquals(1, state.getCurrentTurn());
        assertEquals(Player.PLAYER_1, state.getCurrentPlayer());
        assertNull(state.getChoiceInfo(), "제거한 카드를 소유하는 선택 대기 상태도 없다");
    }

    private List<Card> remainingCards(boolean afterDraw) {
        return afterDraw ? List.of(Card.FEB_3, Card.APR_1) : List.of(Card.FEB_3, Card.MAR_1, Card.APR_1);
    }

    // 덱을 소비하지 않고 검사한다. 모든 변경은 테스트 신호로 멈춰 있어 동시 순회하지 않는다.
    @SuppressWarnings("unchecked")
    private List<Card> storedCards() {
        List<Card> cards = new ArrayList<>();
        for (Player player : List.of(Player.PLAYER_1, Player.PLAYER_2)) {
            cards.addAll(installedCardRepository.getPlayerCards(ROOM_ID, player).block(TIMEOUT));
            cards.addAll(acquiredCardRepository.getAllCards(ROOM_ID, player.getNumber()).block(TIMEOUT));
        }
        cards.addAll(installedCardRepository.getAllRevealedCards(ROOM_ID).block(TIMEOUT));
        Map<Long, Deque<Card>> decks = (Map<Long, Deque<Card>>) ReflectionTestUtils.getField(installedCardRepository, "hiddenDeck");
        cards.addAll(decks.get(ROOM_ID));
        return cards.stream().sorted().toList();
    }

    private void assertCode(Throwable error, WebSocketErrorCode expected) {
        assertEquals(expected, assertInstanceOf(WebSocketBusinessException.class, error).getWebsocketErrorCode());
    }
}
