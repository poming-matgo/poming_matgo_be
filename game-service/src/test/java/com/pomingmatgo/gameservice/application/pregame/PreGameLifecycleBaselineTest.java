package com.pomingmatgo.gameservice.application.pregame;

import com.pomingmatgo.gameservice.application.game.*;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.application.room.RoomReadyService;
import com.pomingmatgo.gameservice.application.room.RoomService;
import com.pomingmatgo.gameservice.domain.*;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.score.PayoutCalculator;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryGameLockAspect;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
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
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.awaitility.Awaitility.await;

@DisplayName("시작 트리거 없이 준비 흐름 실행 소유권과 송신 분리")
class PreGameLifecycleBaselineTest {
    private static final long ROOM_ID = 940_045L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private static final List<Card> DECK = Stream.of(Card.values())
            .sorted(Comparator.comparingInt(card -> card.ordinal() % 4)).toList();

    private final RoomTimerLifecycle lifecycle = new RoomTimerLifecycle();
    private final InMemoryRoomExecutionGate gate = new InMemoryRoomExecutionGate();
    private final InMemoryGameActionExecutor executor = new InMemoryGameActionExecutor(gate, this::onFailure);
    private final InMemoryGameStateRepository states = new InMemoryGameStateRepository(lifecycle, gate);
    private final InMemoryInstalledCardRepository cards = new InMemoryInstalledCardRepository();
    private final InMemoryLeadingPlayerRepository leaders = spy(new InMemoryLeadingPlayerRepository());
    private final SessionManager sessions = new SessionManager();
    private final AcquiredCardRepository acquired = mock(AcquiredCardRepository.class);
    private final RoomCleanupService cleanup = new RoomCleanupService(states, cards, acquired, leaders,
            executor, event -> {}, sessions);
    private final PreGameService preGame = spy(new PreGameService(leaders, cards, states));
    private final GameMessageSender sender = mock(GameMessageSender.class, invocation -> Mono.empty());
    private final TurnScheduler scheduler = mock(TurnScheduler.class);
    private final TurnFlowService turns = new TurnFlowService(lifecycle, mock(GamePlayService.class), sender,
            mock(GameNotificationService.class), mock(PayoutCalculator.class), sessions);
    private PreGameFlowService flow;
    private final Sinks.Empty<Void> sendRelease = Sinks.empty();
    private final GameState initial = GameState.builder().roomId(ROOM_ID)
            .phase(GamePhase.DETERMINING_STARTING_PLAYER).build();

    private void onFailure(Object event) {
        cleanup.onGameActionFailed((com.pomingmatgo.gameservice.domain.event.GameActionFailedEvent) event);
    }

    @BeforeEach
    void setUp() {
        AspectJProxyFactory proxy = new AspectJProxyFactory(new PreGameStartService(states, preGame, turns, lifecycle, scheduler));
        proxy.addAspect(new InMemoryGameLockAspect(executor));
        flow = new PreGameFlowService(proxy.getProxy(), sender, turns);
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
    @DisplayName("선택 조회는 반복해도 선점을 소비하지 않고 미완료 선택은 시작하지 않는다")
    void selectionQueryDoesNotClaimStart() {
        assertFalse(preGame.checkAllSelected(ROOM_ID).block(TIMEOUT));
        leaders.savePlayerMonth(ROOM_ID, Player.PLAYER_1, 0).block(TIMEOUT);
        StepVerifier.create(flow.processLeaderSelection(initial, Player.PLAYER_1, 0))
                .expectComplete().verify(TIMEOUT);
        verify(leaders, never()).tryClaimLeaderSelectionTrigger(ROOM_ID);
        verify(preGame, never()).distributeCards(ROOM_ID);
        leaders.savePlayerMonth(ROOM_ID, Player.PLAYER_2, 2).block(TIMEOUT);
        assertTrue(preGame.checkAllSelected(ROOM_ID).block(TIMEOUT));
        assertTrue(preGame.checkAllSelected(ROOM_ID).block(TIMEOUT));
        verify(leaders, never()).tryClaimLeaderSelectionTrigger(ROOM_ID);
    }

    @Test
    @DisplayName("선택이 완료돼도 프로파일 선점이 거부하면 분배와 첫 턴을 실행하지 않는다")
    void rejectedProfileClaimPreventsStart() {
        doReturn(Mono.just(false)).when(leaders).tryClaimLeaderSelectionTrigger(ROOM_ID);
        StepVerifier.create(selectSecondPlayer()).expectComplete().verify(TIMEOUT);
        assertTrue(preGame.checkAllSelected(ROOM_ID).block(TIMEOUT));
        verify(leaders).tryClaimLeaderSelectionTrigger(ROOM_ID);
        verify(preGame, never()).distributeCards(ROOM_ID);
        verify(preGame, never()).setFirstTurn(any());
        verifyNoInteractions(scheduler);
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
    }

    @Test
    @DisplayName("정상 대조군: 두 번째 선택은 분배·첫 턴 저장·타이머 등록을 완료한다")
    void normalSelectionStartsFirstTurn() {
        StepVerifier.create(selectSecondPlayer()).expectComplete().verify(TIMEOUT);
        assertStarted();
        assertDealt();
        verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_2),
                anyLong(), eq(GamePhase.IN_PROGRESS));
        assertTrue(preGame.checkAllSelected(ROOM_ID).block(TIMEOUT));
        assertRepeatedSelectionRejected();
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
        assertTrue(preGame.checkAllSelected(ROOM_ID).block(TIMEOUT), "선택 조회는 시작 권한을 소비하지 않는다");
        assertRepeatedSelectionRejected();
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
    @DisplayName("분배 저장 대기 중 정리는 시작 완료를 기다리고 이후 재생성을 허용한다")
    void cleanupWaitsForDelayedDeal(boolean recreate) {
        doReturn(sendRelease.asMono().then(Mono.defer(() -> preGame.distributeCards(ROOM_ID, DECK))))
                .when(preGame).distributeCards(ROOM_ID);
        var verification = StepVerifier.create(selectSecondPlayer())
                .then(this::awaitSend)
                .then(() -> {
                    var cleaning = cleanup.cleanupRoom(ROOM_ID).toFuture();
                    assertFalse(cleaning.isDone());
                    assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
                    StepVerifier.create(states.create(GameState.createEmptyRoom(ROOM_ID)))
                            .expectError(BusinessException.class).verify(TIMEOUT);
                    sendRelease.tryEmitEmpty();
                    await().atMost(TIMEOUT).until(cleaning::isDone);
                    cleaning.join();
                    if (recreate) states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
                });
        verification.expectComplete().verify(TIMEOUT);
        assertEquals(List.of(), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        assertEquals(List.of(), cards.getAllRevealedCards(ROOM_ID).block(TIMEOUT));
        if (recreate) assertEquals(GamePhase.NONE, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        else assertNull(states.findById(ROOM_ID).block(TIMEOUT));
        verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_2),
                anyLong(), eq(GamePhase.IN_PROGRESS));
    }

    @Test
    @DisplayName("분배 저장 중 호출자 취소 뒤에도 첫 턴과 타이머까지 완료한다")
    void cancellationDuringDealPreservesStart() {
        doReturn(sendRelease.asMono().then(Mono.defer(() -> preGame.distributeCards(ROOM_ID, DECK))))
                .when(preGame).distributeCards(ROOM_ID);
        StepVerifier.create(selectSecondPlayer()).then(this::awaitSend).thenCancel().verify(TIMEOUT);
        assertEquals(1, sendRelease.currentSubscriberCount());
        sendRelease.tryEmitEmpty();
        assertStarted();
        assertDealt();
        verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_2),
                anyLong(), eq(GamePhase.IN_PROGRESS));
        verifyNoInteractions(sender);
    }

    @Test
    @DisplayName("락 안에서 이미 시작한 phase를 재검증하고 중복 분배하지 않는다")
    void staleStateCannotStartAgain() {
        selectSecondPlayer().block(TIMEOUT);
        StepVerifier.create(selectSecondPlayer()).expectError(WebSocketBusinessException.class).verify(TIMEOUT);
        verify(preGame, times(1)).distributeCards(ROOM_ID);
        assertStarted();
    }

    @ParameterizedTest(name = "호출자 취소={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("첫 턴 저장 대기와 완료 뒤 반복 선택은 트리거 없이도 중복 시작하지 않는다")
    void firstTurnSaveKeepsStartExclusive(boolean cancelCaller) throws Exception {
        doAnswer(invocation -> {
            Mono<GameState> saving = ((Mono<?>) invocation.callRealMethod()).cast(GameState.class);
            return sendRelease.asMono().then(saving);
        }).when(preGame).setFirstTurn(any());
        var selecting = selectSecondPlayer().toFuture();
        try {
            awaitSend();
            if (cancelCaller) selecting.cancel(false);
            assertEquals(1, sendRelease.currentSubscriberCount());
            assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
            StepVerifier.create(selectSecondPlayer())
                    .expectErrorSatisfies(error -> assertEquals(WebSocketErrorCode.TRY_AGAIN,
                            assertInstanceOf(WebSocketBusinessException.class, error).getWebsocketErrorCode()))
                    .verify(TIMEOUT);
            verifyNoInteractions(scheduler, sender);
            sendRelease.tryEmitEmpty();
            if (!cancelCaller) selecting.get(TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            assertStarted();
            assertRepeatedSelectionRejected();
            verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_2),
                    anyLong(), eq(GamePhase.IN_PROGRESS));
        } finally {
            sendRelease.tryEmitEmpty();
        }
    }

    @Test
    @DisplayName("첫 번째 선택은 안내만 한 번 보내고 두 번째 선택이 시작한다")
    void firstSelectionOnlyNotifiesOnce() {
        leaders.cleanup(ROOM_ID).block(TIMEOUT);
        leaders.saveSelectedCard(List.of(Card.JAN_1, Card.FEB_1), ROOM_ID).block(TIMEOUT);
        flow.processLeaderSelection(initial, Player.PLAYER_1, 0).block(TIMEOUT);
        verify(sender, times(1)).sendLeaderSelectionMessage(ROOM_ID, Player.PLAYER_1, 0);
        verify(preGame, never()).distributeCards(ROOM_ID);
        selectSecondPlayer().block(TIMEOUT);
        assertStarted();
    }

    @Test
    @DisplayName("트리거 저장 대기 중 취소도 트리거와 첫 턴 사이를 끊지 않는다")
    void cancellationDuringTriggerClaimPreservesStart() {
        doReturn(sendRelease.asMono().thenReturn(true)).when(preGame).checkAllSelected(ROOM_ID);
        StepVerifier.create(selectSecondPlayer()).then(this::awaitSend).thenCancel().verify(TIMEOUT);
        assertEquals(1, sendRelease.currentSubscriberCount());
        sendRelease.tryEmitEmpty();
        assertStarted();
        assertDealt();
        verifyNoInteractions(sender);
    }

    @Test
    @DisplayName("호출자 취소 후에도 정리는 진행 중 분배가 끝날 때까지 기다린다")
    void cleanupWaitsAfterCallerCancellation() {
        doReturn(sendRelease.asMono().then(Mono.defer(() -> preGame.distributeCards(ROOM_ID, DECK))))
                .when(preGame).distributeCards(ROOM_ID);
        StepVerifier.create(selectSecondPlayer()).then(this::awaitSend).thenCancel().verify(TIMEOUT);
        var cleaning = cleanup.cleanupRoom(ROOM_ID).toFuture();
        assertFalse(cleaning.isDone());
        StepVerifier.create(selectSecondPlayer()).expectError(WebSocketBusinessException.class).verify(TIMEOUT);
        sendRelease.tryEmitEmpty();
        await().atMost(TIMEOUT).until(cleaning::isDone);
        cleaning.join();
        assertNull(states.findById(ROOM_ID).block(TIMEOUT));
        assertEquals(List.of(), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        verifyNoInteractions(sender);
    }

    @Test
    @DisplayName("분배 오류는 원래 오류를 보존하고 전체 정리를 요청한다")
    void dealFailureCleansRoom() {
        RuntimeException failure = new IllegalStateException("deal failed");
        doReturn(Mono.error(failure)).when(preGame).distributeCards(ROOM_ID);
        StepVerifier.create(selectSecondPlayer()).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        assertNull(states.findById(ROOM_ID).block(TIMEOUT));
        StepVerifier.create(selectSecondPlayer()).expectError(WebSocketBusinessException.class).verify(TIMEOUT);
        verify(preGame).distributeCards(ROOM_ID);
        states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
        verifyNoInteractions(sender, scheduler);
    }

    @Test
    @DisplayName("끝나지 않는 분배는 30초 뒤 취소하고 방을 정리한다")
    void dealTimeoutCleansRoom() {
        doReturn(sendRelease.asMono().then(Mono.defer(() -> preGame.distributeCards(ROOM_ID, DECK))))
                .when(preGame).distributeCards(ROOM_ID);
        StepVerifier.withVirtualTime(this::selectSecondPlayer)
                .then(this::awaitSend)
                .thenAwait(Duration.ofSeconds(30))
                .expectError(TimeoutException.class).verify(TIMEOUT);
        assertEquals(0, sendRelease.currentSubscriberCount());
        assertNull(states.findById(ROOM_ID).block(TIMEOUT));
        StepVerifier.create(selectSecondPlayer()).expectError(WebSocketBusinessException.class).verify(TIMEOUT);
        verify(preGame).distributeCards(ROOM_ID);
        verifyNoInteractions(sender, scheduler);
    }

    @ParameterizedTest(name = "카드 인덱스={0}")
    @ValueSource(ints = {-1, 0, 2})
    @DisplayName("수락 전 잘못된 인덱스·중복 월 선택은 방을 정리하지 않는다")
    void invalidSelectionDoesNotCleanRoom(int cardIndex) {
        StepVerifier.create(flow.processLeaderSelection(initial, Player.PLAYER_2, cardIndex))
                .expectError(cardIndex == 0 ? WebSocketBusinessException.class : IndexOutOfBoundsException.class)
                .verify(TIMEOUT);
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        assertEquals(0, leaders.getPlayerSelectedCard(ROOM_ID).block(TIMEOUT).getPlayer2Month());
        verify(preGame, never()).checkAllSelected(ROOM_ID);
        selectSecondPlayer().block(TIMEOUT);
        assertStarted();
    }

    @Test
    @DisplayName("수락 전 취소는 선택 대기를 회수하고 시작 트리거를 소비하지 않는다")
    void cancellationBeforeAcceptanceDoesNotClaimTrigger() {
        doReturn(sendRelease.asMono().thenReturn(Card.FEB_1)).when(leaders).getCardByIndex(ROOM_ID, 1);
        StepVerifier.create(selectSecondPlayer()).then(this::awaitSend).thenCancel().verify(TIMEOUT);
        assertEquals(0, sendRelease.currentSubscriberCount());
        verify(preGame, never()).checkAllSelected(ROOM_ID);
        assertEquals(0, leaders.getPlayerSelectedCard(ROOM_ID).block(TIMEOUT).getPlayer2Month());
        doCallRealMethod().when(leaders).getCardByIndex(ROOM_ID, 1);
        selectSecondPlayer().block(TIMEOUT);
        assertStarted();
    }

    @ParameterizedTest(name = "두 번째 선택={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("선택 저장 중 호출자 취소는 저장과 후속 시작 처리를 끊지 않는다")
    void cancellationDuringSelectionSavePreservesCompletion(boolean secondSelection) {
        if (!secondSelection) {
            leaders.cleanup(ROOM_ID).block(TIMEOUT);
            leaders.saveSelectedCard(List.of(Card.JAN_1, Card.FEB_1), ROOM_ID).block(TIMEOUT);
        }
        pauseSelectionSave();
        StepVerifier.create(selectSecondPlayer()).then(this::awaitSend).thenCancel().verify(TIMEOUT);
        assertEquals(1, sendRelease.currentSubscriberCount());
        sendRelease.tryEmitEmpty();
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertEquals(2, leaders.getPlayerSelectedCard(ROOM_ID).block(TIMEOUT).getPlayer2Month()));
        if (secondSelection) {
            assertStarted();
            assertDealt();
            verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_2),
                    anyLong(), eq(GamePhase.IN_PROGRESS));
        } else {
            assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
            verify(preGame, never()).distributeCards(ROOM_ID);
            verifyNoInteractions(scheduler);
        }
        verify(preGame).checkAllSelected(ROOM_ID);
        verifyNoInteractions(sender);
    }

    @Test
    @DisplayName("선택 저장 중 취소 뒤 정리는 후속 시작까지 기다리고 새 방에 선택을 남기지 않는다")
    void cleanupWaitsForSelectionSaveAfterCancellation() {
        pauseSelectionSave();
        StepVerifier.create(selectSecondPlayer()).then(this::awaitSend).thenCancel().verify(TIMEOUT);
        var cleaning = cleanup.cleanupRoom(ROOM_ID).toFuture();
        assertFalse(cleaning.isDone());
        StepVerifier.create(states.create(GameState.createEmptyRoom(ROOM_ID)))
                .expectError(BusinessException.class).verify(TIMEOUT);
        sendRelease.tryEmitEmpty();
        await().atMost(TIMEOUT).until(cleaning::isDone);
        cleaning.join();
        assertNull(states.findById(ROOM_ID).block(TIMEOUT));
        states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
        assertEquals(0, leaders.getPlayerSelectedCard(ROOM_ID).block(TIMEOUT).getPlayer2Month());
        assertEquals(List.of(), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        verify(preGame).distributeCards(ROOM_ID);
        verifyNoInteractions(sender);
    }

    @Test
    @DisplayName("선택 저장 오류는 원래 오류를 보존하고 부분 선택을 방과 함께 정리한다")
    void selectionSaveFailureCleansRoom() {
        RuntimeException failure = new IllegalStateException("selection save failed");
        Mono<Void> save = leaders.savePlayerMonth(ROOM_ID, Player.PLAYER_2, 2);
        doReturn(save.then(Mono.error(failure))).when(leaders).savePlayerMonth(ROOM_ID, Player.PLAYER_2, 2);
        StepVerifier.create(selectSecondPlayer()).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        assertNull(states.findById(ROOM_ID).block(TIMEOUT));
        assertEquals(0, leaders.getPlayerSelectedCard(ROOM_ID).block(TIMEOUT).getPlayer2Month());
        verify(preGame, never()).checkAllSelected(ROOM_ID);
        verifyNoInteractions(sender, scheduler);
    }

    @Test
    @DisplayName("끝나지 않는 선택 저장은 30초 뒤 취소하고 방을 정리한다")
    void selectionSaveTimeoutCleansRoom() {
        pauseSelectionSave();
        StepVerifier.withVirtualTime(this::selectSecondPlayer)
                .then(this::awaitSend).thenAwait(Duration.ofSeconds(30))
                .expectError(TimeoutException.class).verify(TIMEOUT);
        assertEquals(0, sendRelease.currentSubscriberCount());
        assertNull(states.findById(ROOM_ID).block(TIMEOUT));
        verify(preGame, never()).checkAllSelected(ROOM_ID);
        verifyNoInteractions(sender, scheduler);
    }

    private void pauseSelectionSave() {
        Mono<Void> save = leaders.savePlayerMonth(ROOM_ID, Player.PLAYER_2, 2);
        doReturn(sendRelease.asMono().then(save)).when(leaders).savePlayerMonth(ROOM_ID, Player.PLAYER_2, 2);
    }

    @Test
    @DisplayName("선택 저장 중 Join·Leave·Ready·선택은 같은 락에서 거부하고 다른 방은 진행한다")
    void selectionSerializesWithRoomAdmission() {
        AspectJProxyFactory roomProxy = new AspectJProxyFactory(new RoomService(states, sessions, cleanup));
        roomProxy.addAspect(new InMemoryGameLockAspect(executor));
        RoomService rooms = roomProxy.getProxy();
        AspectJProxyFactory readyProxy = new AspectJProxyFactory(new RoomReadyService(rooms, preGame));
        readyProxy.addAspect(new InMemoryGameLockAspect(executor));
        RoomReadyService ready = readyProxy.getProxy();
        long otherRoom = ROOM_ID + 1;
        states.create(GameState.createEmptyRoom(otherRoom)).block(TIMEOUT);
        pauseSelectionSave();
        var selecting = selectSecondPlayer().toFuture();
        try {
            awaitSend();
            for (Mono<?> competing : List.of(rooms.joinRoom(101L, ROOM_ID), rooms.leaveRoom(101L, ROOM_ID),
                    ready.readyAndPrepare(ROOM_ID, Player.PLAYER_1, false, () -> {}), selectSecondPlayer())) {
                StepVerifier.create(competing)
                        .expectErrorSatisfies(error -> assertEquals(WebSocketErrorCode.TRY_AGAIN,
                                assertInstanceOf(WebSocketBusinessException.class, error).getWebsocketErrorCode()))
                        .verify(TIMEOUT);
            }
            rooms.joinRoom(101L, otherRoom).block(TIMEOUT);
            assertTrue(states.findById(otherRoom).block(TIMEOUT).hasUser(101L));
            assertFalse(selecting.isDone());
            assertEquals(0, leaders.getPlayerSelectedCard(ROOM_ID).block(TIMEOUT).getPlayer2Month());
            sendRelease.tryEmitEmpty();
            await().atMost(TIMEOUT).until(selecting::isDone);
            selecting.join();
            assertStarted();
            verify(preGame).distributeCards(ROOM_ID);
            StepVerifier.create(selectSecondPlayer())
                    .expectErrorSatisfies(error -> assertEquals(WebSocketErrorCode.INVALID_GAME_PHASE,
                            assertInstanceOf(WebSocketBusinessException.class, error).getWebsocketErrorCode()))
                    .verify(TIMEOUT);
        } finally {
            sendRelease.tryEmitEmpty();
            states.cleanup(otherRoom).block(TIMEOUT);
        }
    }

    @Test
    @DisplayName("락 경쟁 요청은 선택을 쓰기 전에 거부되고 해제 뒤 재시도할 수 있다")
    void contentionRejectsBeforeSelection() {
        var entry = gate.acquire(ROOM_ID);
        try {
            StepVerifier.create(selectSecondPlayer()).expectError(WebSocketBusinessException.class).verify(TIMEOUT);
            verify(preGame, never()).selectLeaderCard(ROOM_ID, Player.PLAYER_2, 1);
        } finally {
            gate.release(entry);
        }
        selectSecondPlayer().block(TIMEOUT);
        assertStarted();
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

    private void assertRepeatedSelectionRejected() {
        StepVerifier.create(selectSecondPlayer())
                .expectErrorSatisfies(error -> assertEquals(WebSocketErrorCode.INVALID_GAME_PHASE,
                        assertInstanceOf(WebSocketBusinessException.class, error).getWebsocketErrorCode()))
                .verify(TIMEOUT);
        verify(preGame).distributeCards(ROOM_ID);
        verify(preGame).setFirstTurn(any());
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
