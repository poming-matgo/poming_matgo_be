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

// 결함 재현 기준선이다. 보호 구현 시 기대값을 취소 후 완료·낡은 쓰기 거부로 전환한다.
@DisplayName("첫 턴 전 송신 취소·정리 경합 기준선 (미해결 동작 재현)")
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
    }

    @ParameterizedTest(name = "분배 안내 중 취소={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("재현: 트리거 획득 뒤 송신 취소는 첫 턴 없이 준비 상태를 남긴다")
    void cancellationAfterClaimStrandsGameStart(boolean afterDeal) {
        pauseSend(afterDeal);
        StepVerifier.create(selectSecondPlayer())
                .then(this::awaitSend)
                .thenCancel().verify(TIMEOUT);

        assertEquals(0, sendRelease.currentSubscriberCount());
        sendRelease.tryEmitEmpty();
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        assertFalse(preGame.checkAllSelected(ROOM_ID).block(TIMEOUT), "후속 진행 트리거는 이미 소비됐다");
        if (afterDeal) assertDealt();
        else assertEquals(List.of(), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        verifyNoInteractions(scheduler);
        verify(sender, never()).sendTurnInfo(any(), anyLong());
    }

    @ParameterizedTest(name = "같은 ID 재생성={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("재현: 정리 뒤 늦은 분배는 고아 카드를 남기거나 새 방 상태를 덮어쓴다")
    void delayedDealWritesAfterCleanup(boolean recreate) {
        pauseSend(false);
        var verification = StepVerifier.create(selectSecondPlayer())
                .then(this::awaitSend)
                .then(() -> {
                    cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
                    assertNull(states.findById(ROOM_ID).block(TIMEOUT));
                    assertEquals(List.of(), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
                    assertEquals(1, sendRelease.currentSubscriberCount(), "정리는 진행 중 준비 흐름을 기다리지 않는다");
                    if (recreate) states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
                    assertEquals(Sinks.EmitResult.OK, sendRelease.tryEmitEmpty());
                });
        if (recreate) verification.expectComplete().verify(TIMEOUT);
        else verification.expectError(BusinessException.class).verify(TIMEOUT);

        assertDealt();
        if (recreate) assertStarted();
        else assertNull(states.findById(ROOM_ID).block(TIMEOUT), "삭제된 상태 자체는 부활하지 않는다");
        verifyNoInteractions(scheduler); // 이전 수명의 타이머만 차단되고 카드·상태 쓰기는 차단되지 않는다.
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
