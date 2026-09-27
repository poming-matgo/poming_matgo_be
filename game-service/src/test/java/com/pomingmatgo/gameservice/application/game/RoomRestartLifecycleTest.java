package com.pomingmatgo.gameservice.application.game;

import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryGameStateRepository;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryInstalledCardRepository;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import com.pomingmatgo.gameservice.infrastructure.lock.RoomLockManager;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RoomRestartLifecycleTest {
    private static final long ROOM_ID = 19L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final InMemoryRoomExecutionGate gate = new InMemoryRoomExecutionGate();
    private final InMemoryGameActionExecutor executor = new InMemoryGameActionExecutor(gate, event -> {});
    private final InMemoryGameStateRepository state = spy(new InMemoryGameStateRepository(new RoomTimerLifecycle(), gate));
    private final InMemoryInstalledCardRepository cards = spy(new InMemoryInstalledCardRepository());
    private final AcquiredCardRepository acquired = mock(AcquiredCardRepository.class);
    private final LeadingPlayerRepository leader = mock(LeadingPlayerRepository.class);
    private final RoomLockManager roomLock = mock(RoomLockManager.class);
    private final SessionManager sessions = spy(new SessionManager());
    private final RoomCleanupService cleanup = new RoomCleanupService(state, cards, acquired, leader, roomLock,
            executor, mock(ApplicationEventPublisher.class), sessions);
    private final Sinks.Empty<Void> pause = Sinks.empty();

    @BeforeEach
    void setUp() {
        when(acquired.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(leader.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(roomLock.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        state.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
        cards.savePlayerCards(List.of(Card.JAN_3), ROOM_ID, Player.PLAYER_1).block(TIMEOUT);
        clearInvocations(state);
    }

    @AfterEach
    void tearDown() {
        executor.shutdown();
        cleanup.shutdown();
        sessions.shutdown();
        assertNoOwnedExecutions();
    }

    @ParameterizedTest(name = "재생성 중 취소={0}")
    @ValueSource(booleans = {false, true})
    void acceptedRestartCompletesAfterCallerCancellation(boolean duringCreate) {
        pauseRestart(duringCreate);
        StepVerifier.create(cleanup.restartRoom(ROOM_ID))
                .then(this::assertPausedAndExclusive)
                .thenCancel().verify(TIMEOUT);
        assertEquals(1, pause.currentSubscriberCount());
        assertPausedAndExclusive();
        assertEquals(Sinks.EmitResult.OK, pause.tryEmitEmpty());
        assertRecreated();
        verify(sessions, never()).removeRoom(anyLong());
        assertNoOwnedExecutions();
    }

    @Test
    void normalRestartCompletesOnlyAfterRecreation() {
        pauseRestart(false);
        StepVerifier.create(cleanup.restartRoom(ROOM_ID))
                .then(this::assertPausedAndExclusive)
                .then(() -> pause.tryEmitEmpty())
                .verifyComplete();
        assertRecreated();
        assertNoOwnedExecutions();
    }

    @Test
    void cleanupAlreadyInProgressRejectsRestartBeforeDeletion() {
        var deletion = executor.withCleanup(ROOM_ID, pause::asMono).subscribe();
        try {
            StepVerifier.create(cleanup.restartRoom(ROOM_ID))
                    .expectError(WebSocketBusinessException.class).verify(TIMEOUT);
            verify(cards, never()).cleanup(ROOM_ID);
            verify(state, never()).create(any(GameState.class));
            assertNotNull(state.findById(ROOM_ID).block(TIMEOUT));
        } finally {
            pause.tryEmitEmpty();
            deletion.dispose();
        }
    }

    @Test
    void fullCleanupWaitsForRestartAndDoesNotLeaveRecreatedRoom() {
        pauseRestart(true);
        var caller = cleanup.restartRoom(ROOM_ID).subscribe();
        caller.dispose();
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .then(() -> {
                    verify(cards).cleanup(ROOM_ID);
                    verify(sessions, never()).removeRoom(ROOM_ID);
                    assertPausedAndExclusive();
                    pause.tryEmitEmpty();
                }).verifyComplete();
        assertNull(state.findById(ROOM_ID).block(TIMEOUT));
        verify(cards, times(2)).cleanup(ROOM_ID);
        verify(sessions).removeRoom(ROOM_ID);
        assertNoOwnedExecutions();
        assertEquals(ROOM_ID, state.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT));
    }

    @ParameterizedTest(name = "재생성 오류={0}")
    @ValueSource(booleans = {false, true})
    void restartFailureIsObservedAndBlocksRoomUntilCleanup(boolean duringCreate) {
        pauseRestart(duringCreate);
        StepVerifier.create(cleanup.restartRoom(ROOM_ID))
                .then(() -> pause.tryEmitError(new IllegalStateException("restart failed")))
                .expectErrorMessage("restart failed").verify(TIMEOUT);
        assertNoOwnedExecutions();
        assertThrows(WebSocketBusinessException.class, () -> gate.acquire(ROOM_ID));
        doCallRealMethod().when(cards).cleanup(ROOM_ID);
        doCallRealMethod().when(state).create(any(GameState.class));
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
        assertEquals(ROOM_ID, state.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT));
    }

    @Test
    void shutdownCancelsOwnedRestartAndRejectsLaterRestart() {
        pauseRestart(false);
        StepVerifier.create(cleanup.restartRoom(ROOM_ID))
                .then(executor::shutdown)
                .expectError(CancellationException.class).verify(TIMEOUT);
        assertEquals(0, pause.currentSubscriberCount());
        pause.tryEmitEmpty();
        assertNull(state.findById(ROOM_ID).block(TIMEOUT));
        verify(state, never()).create(any(GameState.class));
        StepVerifier.create(cleanup.restartRoom(ROOM_ID))
                .expectErrorMessage("Game action execution stopped").verify(TIMEOUT);
    }

    @ParameterizedTest(name = "서버 종료={0}")
    @ValueSource(booleans = {false, true})
    void restartInsideAcceptedActionSharesFailureAndShutdownOwnership(boolean shutdown) {
        pauseRestart(false);
        Mono<Object> action = executor.execute(ROOM_ID, () -> GameActionAcceptance.beforeMutation(() ->
                cleanup.restartRoom(ROOM_ID).thenReturn((Object) Boolean.TRUE)));
        StepVerifier.create(action)
                .then(() -> {
                    assertPausedAndExclusive();
                    if (shutdown) executor.shutdown();
                    else pause.tryEmitError(new IllegalStateException("nested restart failed"));
                })
                .expectError(shutdown ? CancellationException.class : IllegalStateException.class)
                .verify(TIMEOUT);
        assertEquals(0, pause.currentSubscriberCount());
        verify(state, never()).create(any(GameState.class));
        assertNoOwnedExecutions();
        if (!shutdown) assertThrows(WebSocketBusinessException.class, () -> gate.acquire(ROOM_ID));
    }

    @Test
    void completedActionsContextCannotAuthorizeLaterRestart() {
        var captured = new java.util.concurrent.atomic.AtomicReference<reactor.util.context.ContextView>();
        executor.execute(ROOM_ID, () -> GameActionAcceptance.beforeMutation(() -> Mono.deferContextual(context -> {
            captured.set(context);
            return Mono.just((Object) Boolean.TRUE);
        }))).block(TIMEOUT);
        StepVerifier.create(cleanup.restartRoom(ROOM_ID).contextWrite(captured.get()))
                .expectError(IllegalStateException.class).verify(TIMEOUT);
        verify(cards, never()).cleanup(ROOM_ID);
        assertEquals(List.of(Card.JAN_3), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
    }

    @SuppressWarnings("unchecked")
    private void pauseRestart(boolean duringCreate) {
        if (duringCreate) {
            doAnswer(invocation -> {
                Mono<Long> creation = (Mono<Long>) invocation.callRealMethod();
                return pause.asMono().then(creation);
            }).when(state).create(any(GameState.class));
        } else {
            doReturn(pause.asMono().then(cards.cleanup(ROOM_ID))).when(cards).cleanup(ROOM_ID);
        }
    }

    private void assertPausedAndExclusive() {
        assertEquals(1, pause.currentSubscriberCount());
        assertNull(state.findById(ROOM_ID).block(TIMEOUT));
        assertThrows(WebSocketBusinessException.class, () -> gate.acquire(ROOM_ID));
        assertThrows(BusinessException.class, () -> gate.create(ROOM_ID, () -> ROOM_ID));
        var otherRoom = gate.acquire(ROOM_ID + 1);
        gate.release(otherRoom);
        gate.discardIdle(ROOM_ID + 1);
    }

    private void assertRecreated() {
        assertEquals(GameState.createEmptyRoom(ROOM_ID).getPhase(), state.findById(ROOM_ID).block(TIMEOUT).getPhase());
        assertEquals(List.of(), cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        verify(state).create(any(GameState.class));
        var nextAction = gate.acquire(ROOM_ID);
        gate.release(nextAction);
    }

    private void assertNoOwnedExecutions() {
        assertTrue(((Set<?>) ReflectionTestUtils.getField(executor, "executions")).isEmpty());
    }
}
