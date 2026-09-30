package com.pomingmatgo.gameservice.application.game;

import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.event.GameActionFailedEvent;
import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryGameStateRepository;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryInstalledCardRepository;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.core.Exceptions;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GameActionFailureCleanupTest {
    private static final long ROOM_ID = 21L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final InMemoryRoomExecutionGate gate = new InMemoryRoomExecutionGate();
    private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
    private final InMemoryGameActionExecutor executor = new InMemoryGameActionExecutor(gate, events);
    private final InMemoryGameStateRepository state = spy(new InMemoryGameStateRepository(new RoomTimerLifecycle(), gate));
    private final InMemoryInstalledCardRepository cards = spy(new InMemoryInstalledCardRepository());
    private final AcquiredCardRepository acquired = mock(AcquiredCardRepository.class);
    private final LeadingPlayerRepository leader = mock(LeadingPlayerRepository.class);
    private final SessionManager sessions = mock(SessionManager.class);
    private final RoomCleanupService cleanup = new RoomCleanupService(state, cards, acquired, leader,
            executor, events, sessions);
    private final IllegalStateException failure = new IllegalStateException("mutation failed");

    @BeforeEach
    void setUp() {
        when(acquired.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(leader.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(sessions.removeRoom(ROOM_ID)).thenReturn(Mono.empty());
        doAnswer(call -> {
            cleanup.onGameActionFailed(call.getArgument(0));
            return null;
        }).when(events).publishEvent(any(GameActionFailedEvent.class));
        state.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
    }

    @AfterEach
    void tearDown() {
        executor.shutdown();
        cleanup.shutdown();
        assertTrue(((Set<?>) ReflectionTestUtils.getField(executor, "executions")).isEmpty());
        assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(cleanup, "executions")).isEmpty());
    }

    @Test
    void acceptedErrorCleansDataAndSessionsBeforeReuseAndPreservesOriginalError() {
        StepVerifier.create(accepted(Mono.error(failure))).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        assertNull(state.findById(ROOM_ID).block(TIMEOUT));
        verify(cards).cleanup(ROOM_ID);
        verify(acquired).cleanup(ROOM_ID);
        verify(leader).cleanup(ROOM_ID);
        verify(sessions).removeRoom(ROOM_ID);
        verify(events).publishEvent(new RoomCleanedUpEvent(ROOM_ID));
        assertReusable();
    }

    @Test
    void pendingCleanupBlocksReuseButDoesNotDelayOriginalErrorOrOtherRooms() {
        Sinks.Empty<Void> pause = Sinks.empty();
        when(acquired.cleanup(ROOM_ID)).thenReturn(pause.asMono());
        StepVerifier.create(accepted(Mono.error(failure))).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        assertEquals(1, pause.currentSubscriberCount());
        assertBlocked();
        StepVerifier.create(executor.execute(22, () -> Mono.just("other"))).expectNext("other").verifyComplete();
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID)).thenCancel().verify(TIMEOUT);
        assertEquals(1, pause.currentSubscriberCount());
        pause.tryEmitEmpty();
        verify(acquired).cleanup(ROOM_ID);
        verify(sessions).removeRoom(ROOM_ID);
        assertReusable();
    }

    @Test
    void existingCleanupMergesFailureBeforeGateReleaseAndDoesNotDeleteRecreatedRoom() {
        Sinks.One<Object> action = Sinks.one();
        StepVerifier.create(accepted(action.asMono()))
                .then(() -> assertEquals(1, action.currentSubscriberCount()))
                .thenCancel().verify(TIMEOUT);
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID).doOnSuccess(ignored ->
                        state.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT)))
                .then(() -> {
                    verify(state, never()).cleanup(ROOM_ID);
                    action.tryEmitError(failure);
                }).verifyComplete();
        verify(state).cleanup(ROOM_ID);
        assertNotNull(state.findById(ROOM_ID).block(TIMEOUT));
        assertReusable();
    }

    @ParameterizedTest(name = "대기 중 세션 정리 실패={0}")
    @ValueSource(booleans = {false, true})
    void sessionCleanupKeepsGateUntilCompletionEvenAfterObserverCancellation(boolean sessionFailure) {
        Sinks.Empty<Void> pendingSession = Sinks.empty();
        var sessionError = new IllegalStateException("late session cleanup failure");
        when(sessions.removeRoom(ROOM_ID)).thenReturn(pendingSession.asMono());

        StepVerifier.create(accepted(Mono.error(failure)))
                .expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        assertNull(state.findById(ROOM_ID).block(TIMEOUT));
        assertEquals(1, pendingSession.currentSubscriberCount());
        assertBlocked();
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID)).thenCancel().verify(TIMEOUT);
        assertEquals(1, pendingSession.currentSubscriberCount());
        assertBlocked();

        var verification = StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .then(() -> {
                    if (sessionFailure) assertEquals(Sinks.EmitResult.OK, pendingSession.tryEmitError(sessionError));
                    else assertEquals(Sinks.EmitResult.OK, pendingSession.tryEmitEmpty());
                });
        if (sessionFailure) verification.expectErrorMatches(error -> error == sessionError).verify(TIMEOUT);
        else verification.expectComplete().verify(TIMEOUT);
        verify(state).cleanup(ROOM_ID);
        verify(sessions).removeRoom(ROOM_ID);
        if (sessionFailure) {
            assertBlocked();
            when(sessions.removeRoom(ROOM_ID)).thenReturn(Mono.empty());
            cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
        }
        assertReusable();
    }

    @ParameterizedTest(name = "세션 정리 실패={0}")
    @ValueSource(booleans = {false, true})
    void cleanupFailureKeepsRoomBlockedUntilExplicitSuccessfulCleanup(boolean sessionFailure) {
        if (sessionFailure) when(sessions.removeRoom(ROOM_ID)).thenReturn(Mono.error(new IllegalStateException("session failure")));
        else when(acquired.cleanup(ROOM_ID)).thenReturn(Mono.error(new IllegalStateException("card failure")));
        StepVerifier.create(accepted(Mono.error(failure))).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        verify(sessions).removeRoom(ROOM_ID);
        assertBlocked();
        when(sessions.removeRoom(ROOM_ID)).thenReturn(Mono.empty());
        when(acquired.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
        assertReusable();
    }

    @Test
    void acceptedTimeoutAfterCallerCancellationStartsCleanup() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.getOrSet();
        try {
            Sinks.One<Object> pending = Sinks.one();
            StepVerifier.create(accepted(pending.asMono()))
                    .then(() -> assertEquals(1, pending.currentSubscriberCount()))
                    .thenCancel().verify(TIMEOUT);
            clock.advanceTimeBy(Duration.ofSeconds(30));
            assertEquals(0, pending.currentSubscriberCount());
            verify(sessions).removeRoom(ROOM_ID);
            assertNull(state.findById(ROOM_ID).block(TIMEOUT));
            assertReusable();
        } finally {
            VirtualTimeScheduler.reset();
        }
    }

    @ParameterizedTest(name = "안내 timeout={0}")
    @ValueSource(booleans = {false, true})
    void notificationFailureAllowsReuseOnlyAfterRequiredCleanup(boolean timeout) {
        Sinks.Empty<Void> notification = Sinks.empty();
        Sinks.Empty<Void> pendingSession = Sinks.empty();
        when(sessions.removeRoom(ROOM_ID)).thenReturn(pendingSession.asMono());

        StepVerifier.withVirtualTime(() -> cleanup.cleanupRoom(ROOM_ID, notification.asMono()))
                .then(() -> {
                    assertBlocked();
                    verify(state, never()).cleanup(ROOM_ID);
                    if (!timeout) notification.tryEmitError(new IllegalStateException("send failed"));
                })
                .thenAwait(Duration.ofSeconds(5))
                .then(() -> {
                    assertEquals(0, notification.currentSubscriberCount());
                    assertNull(state.findById(ROOM_ID).block(TIMEOUT));
                    assertEquals(1, pendingSession.currentSubscriberCount());
                    assertBlocked();
                    pendingSession.tryEmitEmpty();
                })
                .expectComplete().verify(TIMEOUT);
        verify(state).cleanup(ROOM_ID);
        verify(sessions).removeRoom(ROOM_ID);
        assertReusable();
    }

    @ParameterizedTest(name = "안내 timeout={0}")
    @ValueSource(booleans = {false, true})
    void notificationFailurePreservesDataAndSessionFailures(boolean timeout) {
        Sinks.Empty<Void> notification = Sinks.empty();
        var sessionFailure = new IllegalStateException("session cleanup failed");
        when(leader.cleanup(ROOM_ID)).thenReturn(Mono.error(failure));
        when(sessions.removeRoom(ROOM_ID)).thenReturn(Mono.error(sessionFailure));

        StepVerifier.withVirtualTime(() -> cleanup.cleanupRoom(ROOM_ID, notification.asMono()))
                .then(() -> {
                    if (!timeout) notification.tryEmitError(new IllegalStateException("send failed"));
                })
                .thenAwait(Duration.ofSeconds(5))
                .expectErrorSatisfies(error -> {
                    var errors = Exceptions.unwrapMultipleExcludingTracebacks(error).stream()
                            .flatMap(cause -> Exceptions.unwrapMultipleExcludingTracebacks(cause).stream()).toList();
                    assertEquals(2, errors.size());
                    assertTrue(errors.contains(failure));
                    assertTrue(errors.contains(sessionFailure));
                }).verify(TIMEOUT);
        assertEquals(0, notification.currentSubscriberCount());
        verify(sessions).removeRoom(ROOM_ID);
        assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(cleanup, "executions")).isEmpty());
        assertBlocked();
    }

    @Test
    void cleanupWaitsForActiveExecutionBeforeDeletingData() {
        // 직접 획득한 gate의 대기 계약이다. 운영 executor의 30초 제한을 재현하지 않는다.
        var active = gate.acquire(ROOM_ID);
        Sinks.Empty<Void> data = Sinks.empty();
        when(acquired.cleanup(ROOM_ID)).thenReturn(data.asMono());
        VirtualTimeScheduler clock = VirtualTimeScheduler.getOrSet();
        AtomicBoolean completed = new AtomicBoolean();
        AtomicReference<Throwable> error = new AtomicReference<>();
        try {
            cleanup.cleanupRoom(ROOM_ID).subscribe(ignored -> {}, error::set, () -> completed.set(true));
            clock.advanceTimeBy(Duration.ofSeconds(60));
            verify(state, never()).cleanup(ROOM_ID);
            assertBlocked();
            gate.release(active);
            assertEquals(1, data.currentSubscriberCount());
            assertFalse(completed.get());
            assertNull(error.get());
            assertEquals(Sinks.EmitResult.OK, data.tryEmitEmpty());
            assertTrue(completed.get());
            assertNull(error.get());
            assertReusable();
        } finally {
            cleanup.shutdown();
            VirtualTimeScheduler.reset();
        }
    }

    @ParameterizedTest(name = "정리 대기 중 실행 수락={0}")
    @ValueSource(booleans = {false, true})
    void cleanupUsesRemainingExecutionDeadlineThenNotificationDeadline(boolean accepted) {
        VirtualTimeScheduler clock = VirtualTimeScheduler.getOrSet();
        Sinks.One<Object> action = Sinks.one();
        Sinks.Empty<Void> notification = Sinks.empty();
        AtomicReference<Throwable> actionError = new AtomicReference<>();
        try {
            var caller = (accepted ? accepted(action.asMono())
                    : executor.execute(ROOM_ID, action::asMono))
                    .subscribe(ignored -> fail("Pending action must not succeed"), actionError::set);
            assertEquals(1, action.currentSubscriberCount());
            if (accepted) caller.dispose();
            clock.advanceTimeBy(Duration.ofSeconds(10));

            StepVerifier.create(cleanup.cleanupRoom(ROOM_ID, notification.asMono()))
                    .thenCancel().verify(TIMEOUT);
            StepVerifier.create(cleanup.cleanupRoom(ROOM_ID,
                            Mono.error(new AssertionError("Duplicate cleanup must keep first notification"))))
                    .then(() -> {
                        clock.advanceTimeBy(Duration.ofSeconds(19));
                        assertEquals(1, action.currentSubscriberCount());
                        assertEquals(0, notification.currentSubscriberCount());
                        verify(state, never()).cleanup(ROOM_ID);
                        assertBlocked();
                        clock.advanceTimeBy(Duration.ofSeconds(1));
                        assertEquals(0, action.currentSubscriberCount());
                        assertEquals(1, notification.currentSubscriberCount());
                        if (accepted) assertNull(actionError.get());
                        else assertInstanceOf(TimeoutException.class, actionError.get());
                        verify(events, times(accepted ? 1 : 0)).publishEvent(any(GameActionFailedEvent.class));
                        clock.advanceTimeBy(Duration.ofSeconds(4));
                        verify(state, never()).cleanup(ROOM_ID);
                        assertBlocked();
                        clock.advanceTimeBy(Duration.ofSeconds(1));
                    })
                    .expectComplete().verify(TIMEOUT);

            assertEquals(0, notification.currentSubscriberCount());
            assertNull(state.findById(ROOM_ID).block(TIMEOUT));
            verify(state).cleanup(ROOM_ID);
            verify(cards).cleanup(ROOM_ID);
            verify(acquired).cleanup(ROOM_ID);
            verify(leader).cleanup(ROOM_ID);
            verify(sessions).removeRoom(ROOM_ID);
            verify(events).publishEvent(new RoomCleanedUpEvent(ROOM_ID));
            assertTrue(((Set<?>) ReflectionTestUtils.getField(executor, "executions")).isEmpty());
            assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(cleanup, "executions")).isEmpty());
            assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(gate, "rooms")).isEmpty());
            state.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
            clock.advanceTimeBy(Duration.ofMinutes(1));
            assertNotNull(state.findById(ROOM_ID).block(TIMEOUT));
            verify(state).cleanup(ROOM_ID);
            assertReusable();
        } finally {
            executor.shutdown();
            cleanup.shutdown();
            VirtualTimeScheduler.reset();
        }
    }

    @Test
    void validationFailureTimeoutAndSuccessDoNotRequestCleanup() {
        StepVerifier.create(executor.execute(ROOM_ID, () -> Mono.error(failure))).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        StepVerifier.withVirtualTime(() -> executor.execute(ROOM_ID, Mono::never))
                .thenAwait(Duration.ofSeconds(30)).expectError(TimeoutException.class).verify(TIMEOUT);
        StepVerifier.create(accepted(Mono.just("ok"))).expectNext("ok").verifyComplete();
        verify(events, never()).publishEvent(any(GameActionFailedEvent.class));
        assertNotNull(state.findById(ROOM_ID).block(TIMEOUT));
    }

    @Test
    void eventDispatchFailurePreservesActionErrorAndRoomBlock() {
        doThrow(new IllegalStateException("listener unavailable")).when(events).publishEvent(any(GameActionFailedEvent.class));
        StepVerifier.create(accepted(Mono.error(failure))).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        assertBlocked();
        assertNotNull(state.findById(ROOM_ID).block(TIMEOUT));
    }

    private Mono<Object> accepted(Mono<Object> operation) {
        return executor.execute(ROOM_ID, () -> GameActionAcceptance.beforeMutation(() -> operation));
    }

    private void assertBlocked() {
        StepVerifier.create(executor.execute(ROOM_ID, () -> Mono.just("blocked")))
                .expectError(WebSocketBusinessException.class).verify(TIMEOUT);
        assertThrows(RuntimeException.class, () -> gate.create(ROOM_ID, () -> "blocked"));
    }

    private void assertReusable() {
        assertEquals("created", gate.create(ROOM_ID, () -> "created"));
        StepVerifier.create(executor.execute(ROOM_ID, () -> Mono.just("next"))).expectNext("next").verifyComplete();
    }
}
