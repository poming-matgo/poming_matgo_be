package com.pomingmatgo.gameservice.application.pregame;

import com.pomingmatgo.gameservice.application.game.*;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.*;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.score.PayoutCalculator;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomLockManager;
import com.pomingmatgo.gameservice.infrastructure.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.*;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import com.pomingmatgo.gameservice.infrastructure.scheduler.TurnScheduler;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.awaitility.Awaitility.await;

// 송신 경계는 정상 보장, 저장 대기 경계는 실행 소유권 도입 전 결함 재현이다.
@DisplayName("준비 흐름 송신 분리 및 저장 경합 기준선")
class PreGameLifecycleBaselineTest {
    private static final long ROOM_ID = 940_045L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private static final List<Card> DECK = Stream.of(Card.values())
            .sorted(Comparator.comparingInt(card -> card.ordinal() % 4)).toList();

    private final RoomTimerLifecycle lifecycle = new RoomTimerLifecycle();
    private final InMemoryRoomExecutionGate gate = new InMemoryRoomExecutionGate();
    private final InMemoryGameActionExecutor executor = new InMemoryGameActionExecutor(gate, event -> {});
    private final InMemoryGameStateRepository states = new InMemoryGameStateRepository(lifecycle, gate);
    private final InMemoryInstalledCardRepository cards = new InMemoryInstalledCardRepository();
    private final InMemoryLeadingPlayerRepository leaders = new InMemoryLeadingPlayerRepository();
    private final InMemoryRoomLockManager roomLock = new InMemoryRoomLockManager();
    private final SessionManager sessions = new SessionManager();
    private final AcquiredCardRepository acquired = mock(AcquiredCardRepository.class);
    private final RoomCleanupService cleanup = new RoomCleanupService(states, cards, acquired, leaders,
            roomLock, executor, event -> {}, sessions);
    private final PreGameService preGame = spy(new PreGameService(leaders, cards, states, roomLock));
    private final GameMessageSender sender = mock(GameMessageSender.class, invocation -> Mono.empty());
    private final TurnScheduler scheduler = mock(TurnScheduler.class);
    private final TurnFlowService turns = new TurnFlowService(lifecycle, mock(GamePlayService.class), sender,
            mock(GameNotificationService.class), mock(PayoutCalculator.class));
    private final PreGameFlowService flow = new PreGameFlowService(lifecycle, preGame, sender, turns, scheduler);
    private final Sinks.Empty<Void> sendRelease = Sinks.empty();
    private final GameState initial = GameState.builder().roomId(ROOM_ID)
            .phase(GamePhase.DETERMINING_STARTING_PLAYER).build();

    @BeforeEach
    void setUp() {
        when(acquired.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        states.create(initial).block(TIMEOUT);
        leaders.saveSelectedCard(List.of(Card.JAN_1, Card.FEB_1), ROOM_ID).block(TIMEOUT);
        preGame.selectLeaderCard(ROOM_ID, Player.PLAYER_1, 0).block(TIMEOUT);
        doAnswer(invocation -> preGame.distributeCards(ROOM_ID, DECK)).when(preGame).distributeCards(ROOM_ID);
    }

    @AfterEach
    void tearDown() {
        sendRelease.tryEmitEmpty();
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
        cleanup.shutdown();
        executor.shutdown();
        sessions.shutdown();
    }

    @Test
    @DisplayName("정상 대조군: 두 번째 선택은 분배·첫 턴 저장·타이머 등록을 완료한다")
    void normalSelectionStartsFirstTurn() {
        StepVerifier.create(selectSecondPlayer()).expectComplete().verify(TIMEOUT);
        assertStarted();
        assertDealt();
        verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_2),
                anyLong(), eq(GamePhase.IN_PROGRESS));
        assertFalse(preGame.checkAllSelected(ROOM_ID).block(TIMEOUT));
        var order = inOrder(sender);
        order.verify(sender).sendLeaderSelectionMessage(ROOM_ID, Player.PLAYER_2, 1);
        order.verify(sender).sendLeaderSelectionResult(eq(ROOM_ID), any());
        order.verify(sender).sendDistributedCardInfo(eq(ROOM_ID), any());
        order.verify(sender).sendTurnInfo(any(), anyLong());
    }

    @ParameterizedTest(name = "분배 안내 중 취소={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("안내 송신 취소 전에 첫 턴과 타이머 등록을 완료한다")
    void cancellationDuringSendPreservesGameStart(boolean afterDeal) {
        pauseSend(afterDeal);
        StepVerifier.create(selectSecondPlayer())
                .then(this::awaitSend)
                .thenCancel().verify(TIMEOUT);

        assertEquals(0, sendRelease.currentSubscriberCount());
        sendRelease.tryEmitEmpty();
        assertStarted();
        assertFalse(preGame.checkAllSelected(ROOM_ID).block(TIMEOUT), "후속 진행 트리거는 이미 소비됐다");
        assertDealt();
        verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_2),
                anyLong(), eq(GamePhase.IN_PROGRESS));
        verify(sender, never()).sendTurnInfo(any(), anyLong());
    }

    @ParameterizedTest(name = "같은 ID 재생성={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("정리 뒤 늦은 안내는 카드나 새 방 상태를 쓰지 않는다")
    void delayedSendDoesNotWriteAfterCleanup(boolean recreate) {
        pauseSend(false);
        var verification = StepVerifier.create(selectSecondPlayer())
                .then(this::awaitSend)
                .then(() -> {
                    cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
                    assertNull(states.findById(ROOM_ID).block(TIMEOUT));
                    assertEquals(List.of(), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
                    assertEquals(1, sendRelease.currentSubscriberCount());
                    if (recreate) states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
                    assertEquals(Sinks.EmitResult.OK, sendRelease.tryEmitEmpty());
                });
        verification.expectComplete().verify(TIMEOUT);

        assertEquals(List.of(), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        assertEquals(List.of(), cards.getAllRevealedCards(ROOM_ID).block(TIMEOUT));
        if (recreate) assertEquals(GamePhase.NONE, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        else assertNull(states.findById(ROOM_ID).block(TIMEOUT), "삭제된 상태 자체는 부활하지 않는다");
        verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_2),
                anyLong(), eq(GamePhase.IN_PROGRESS));
    }

    @ParameterizedTest(name = "같은 ID 재생성={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("미해결 재현: 분배 저장 대기 중 정리는 낡은 카드·상태 쓰기를 막지 못한다")
    void delayedDealStillWritesAfterCleanup(boolean recreate) {
        doReturn(sendRelease.asMono().then(Mono.defer(() -> preGame.distributeCards(ROOM_ID, DECK))))
                .when(preGame).distributeCards(ROOM_ID);
        var verification = StepVerifier.create(selectSecondPlayer())
                .then(this::awaitSend)
                .then(() -> {
                    cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
                    if (recreate) states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
                    sendRelease.tryEmitEmpty();
                });
        if (recreate) verification.expectComplete().verify(TIMEOUT);
        else verification.expectError(BusinessException.class).verify(TIMEOUT);
        assertDealt();
        if (recreate) assertStarted();
        else assertNull(states.findById(ROOM_ID).block(TIMEOUT));
        verifyNoInteractions(scheduler);
    }

    @Test
    @DisplayName("선택 안내 오류도 이미 저장한 첫 턴과 타이머를 취소하지 않는다")
    void selectionSendFailurePreservesGameStart() {
        RuntimeException failure = new IllegalStateException("send failed");
        when(sender.sendLeaderSelectionMessage(anyLong(), any(), anyInt())).thenReturn(Mono.error(failure));
        StepVerifier.create(selectSecondPlayer()).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        assertStarted();
        assertDealt();
        verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_2),
                anyLong(), eq(GamePhase.IN_PROGRESS));
        verify(sender, never()).sendLeaderSelectionResult(anyLong(), any());
    }

    private Mono<Void> selectSecondPlayer() {
        return flow.processLeaderSelection(initial, Player.PLAYER_2, 1);
    }

    private void pauseSend(boolean afterDeal) {
        Mono<Void> paused = sendRelease.asMono();
        if (afterDeal) when(sender.sendDistributedCardInfo(eq(ROOM_ID), any())).thenReturn(paused);
        else when(sender.sendLeaderSelectionResult(eq(ROOM_ID), any())).thenReturn(paused);
    }

    private void awaitSend() {
        await().atMost(TIMEOUT).until(() -> sendRelease.currentSubscriberCount() == 1);
    }

    private void assertStarted() {
        GameState current = states.findById(ROOM_ID).block(TIMEOUT);
        assertNotNull(current);
        assertEquals(GamePhase.IN_PROGRESS, current.getPhase());
        assertEquals(1, current.getRound());
        assertEquals(1, current.getCurrentTurn());
        assertEquals(Player.PLAYER_2, current.getCurrentPlayer());
    }

    private void assertDealt() {
        assertEquals(DECK.subList(0, 10), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        assertEquals(DECK.subList(10, 20), cards.getPlayerCards(ROOM_ID, Player.PLAYER_2).block(TIMEOUT));
        assertEquals(DECK.subList(20, 28).stream().sorted().toList(), cards.getAllRevealedCards(ROOM_ID).block(TIMEOUT));
    }
}
