package com.pomingmatgo.gameservice.application.room;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.infrastructure.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RoomCleanupFailureTest {
    private static final long ROOM_ID = 17L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final GameStateRepository state = mock(GameStateRepository.class);
    private final InstalledCardRepository installed = mock(InstalledCardRepository.class);
    private final AcquiredCardRepository acquired = mock(AcquiredCardRepository.class);
    private final LeadingPlayerRepository leader = mock(LeadingPlayerRepository.class);
    private final GameLockCleaner gameLock = mock(GameLockCleaner.class, CALLS_REAL_METHODS);
    private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
    private final List<String> completed = new ArrayList<>();
    private final RoomCleanupService cleanup = new RoomCleanupService(
            state, installed, acquired, leader, gameLock, events, new SessionManager());

    @BeforeEach
    void setUp() {
        when(state.cleanup(ROOM_ID)).thenReturn(done("state"));
        when(installed.cleanup(ROOM_ID)).thenReturn(done("installed"));
        when(acquired.cleanup(ROOM_ID)).thenReturn(done("acquired"));
        when(leader.cleanup(ROOM_ID)).thenReturn(done("leader"));
        when(gameLock.cleanup(ROOM_ID)).thenReturn(done("gameLock"));
        doAnswer(invocation -> {
            completed.add("event");
            return null;
        }).when(events).publishEvent(new RoomCleanedUpEvent(ROOM_ID));
    }

    @Test
    void successfulCleanupIsLazyAndCompletesAllResources() {
        clearInvocations(state, installed, acquired, leader, gameLock, events);
        Mono<Void> result = cleanup.cleanupRoom(ROOM_ID);
        verifyNoInteractions(state, installed, acquired, leader, gameLock, events);
        StepVerifier.create(result).expectComplete().verify(TIMEOUT);
        assertEquals(List.of("event", "state", "installed", "acquired", "leader", "gameLock"), completed);
    }

    @Test
    void firstFailureStillCleansOtherResourcesAndPublishesEvent() {
        RuntimeException failure = new IllegalStateException("state cleanup failed");
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(failure));
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        assertEquals(List.of("event", "installed", "acquired", "leader", "gameLock"), completed);
    }

    @Test
    void synchronousFailureDoesNotPreventOtherCleanup() {
        RuntimeException failure = new IllegalStateException("cleanup construction failed");
        when(state.cleanup(ROOM_ID)).thenThrow(failure);
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        assertEquals(List.of("event", "installed", "acquired", "leader", "gameLock"), completed);
    }

    @Test
    void multipleFailuresArePreserved() {
        RuntimeException first = new IllegalStateException("state cleanup failed");
        RuntimeException second = new IllegalStateException("lock cleanup failed");
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(first));
        when(gameLock.cleanup(ROOM_ID)).thenReturn(Mono.error(second));
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .expectErrorSatisfies(error -> assertEquals(
                        List.of(first, second), Exceptions.unwrapMultipleExcludingTracebacks(error)))
                .verify(TIMEOUT);
        assertEquals(List.of("event", "installed", "acquired", "leader"), completed);
    }

    @Test
    void errorWaitsForPendingCleanupWithoutCancellingIt() {
        Sinks.Empty<Void> pending = Sinks.empty();
        RuntimeException failure = new IllegalStateException("state cleanup failed");
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(failure));
        when(installed.cleanup(ROOM_ID)).thenReturn(pending.asMono().then(done("installed")));
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .then(() -> {
                    assertEquals(1, pending.currentSubscriberCount());
                    assertTrue(completed.contains("event"));
                    assertEquals(Sinks.EmitResult.OK, pending.tryEmitEmpty());
                })
                .expectErrorSatisfies(error -> {
                    assertSame(failure, error);
                    assertTrue(completed.contains("installed"));
                }).verify(TIMEOUT);
    }

    @Test
    void eventFailureDoesNotCancelPendingRepositoryCleanup() {
        Sinks.Empty<Void> pending = Sinks.empty();
        RuntimeException failure = new IllegalStateException("listener failed");
        when(state.cleanup(ROOM_ID)).thenReturn(pending.asMono().then(done("state")));
        doThrow(failure).when(events).publishEvent(new RoomCleanedUpEvent(ROOM_ID));
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .then(() -> {
                    assertEquals(1, pending.currentSubscriberCount());
                    assertEquals(Sinks.EmitResult.OK, pending.tryEmitEmpty());
                })
                .expectErrorSatisfies(error -> {
                    assertSame(failure, error);
                    assertTrue(completed.contains("state"));
                }).verify(TIMEOUT);
    }

    @Test
    void fullCleanupIsLazyAndSynchronousCompletionReleasesTracking() {
        clearInvocations(state);
        Mono<Void> result = cleanup.cleanupRoom(ROOM_ID);
        verifyNoInteractions(state);
        assertEquals(0, executionCount());
        StepVerifier.create(result).expectComplete().verify(TIMEOUT);
        assertEquals(0, executionCount());
        StepVerifier.create(result).expectComplete().verify(TIMEOUT);
        verify(state, times(2)).cleanup(ROOM_ID);
    }

    @Test
    void overlappingCleanupSharesExecutionEvenAfterFirstObserverCancels() {
        Sinks.Empty<Void> gate = Sinks.empty();
        when(installed.cleanup(ROOM_ID)).thenReturn(gate.asMono());
        var first = cleanup.cleanupRoom(ROOM_ID).subscribe();
        assertEquals(1, executionCount());
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .then(() -> {
                    first.dispose();
                    assertEquals(1, gate.currentSubscriberCount());
                    verify(state).cleanup(ROOM_ID);
                    assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
                })
                .expectComplete().verify(TIMEOUT);
        assertEquals(0, executionCount());
    }

    @Test
    void detachedCleanupFailureStillReleasesTrackingAndReachesOtherObserver() {
        Sinks.Empty<Void> gate = Sinks.empty();
        var failure = new IllegalStateException("late cleanup failure");
        when(installed.cleanup(ROOM_ID)).thenReturn(gate.asMono());
        var first = cleanup.cleanupRoom(ROOM_ID).subscribe();
        first.dispose();
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .then(() -> assertEquals(Sinks.EmitResult.OK, gate.tryEmitError(failure)))
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        assertEquals(0, executionCount());
        assertTrue(completed.contains("gameLock"));
    }

    @Test
    void shutdownCancelsExecutionNotifiesObserverAndRejectsNewCleanup() {
        Sinks.Empty<Void> gate = Sinks.empty();
        when(installed.cleanup(ROOM_ID)).thenReturn(gate.asMono());
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .then(() -> {
                    assertEquals(1, gate.currentSubscriberCount());
                    cleanup.shutdown();
                })
                .expectError(CancellationException.class).verify(TIMEOUT);
        assertEquals(0, gate.currentSubscriberCount());
        assertEquals(0, executionCount());
        cleanup.shutdown();
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .expectError(IllegalStateException.class).verify(TIMEOUT);
        verify(state).cleanup(ROOM_ID);
    }

    @Test
    void shutdownDuringPublisherCreationDoesNotLeaveAnExecution() {
        Sinks.Empty<Void> gate = Sinks.empty();
        when(installed.cleanup(ROOM_ID)).thenAnswer(invocation -> {
            cleanup.shutdown();
            return gate.asMono();
        });
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .expectError(CancellationException.class).verify(TIMEOUT);
        assertEquals(0, gate.currentSubscriberCount());
        assertEquals(0, executionCount());
    }

    @Test
    void synchronousFullCleanupFailureReleasesTracking() {
        var failure = new IllegalStateException("construction failed");
        when(state.cleanup(ROOM_ID)).thenThrow(failure);
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        assertEquals(0, executionCount());
        assertTrue(completed.contains("gameLock"));
    }

    private int executionCount() {
        return ((Map<?, ?>) ReflectionTestUtils.getField(cleanup, "executions")).size();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failureAfterAllObserversCancelIsLoggedAndReleased(boolean notificationFailure) {
        Logger logger = (Logger) LoggerFactory.getLogger(RoomCleanupService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            Sinks.Empty<Void> gate = Sinks.empty();
            if (!notificationFailure) when(installed.cleanup(ROOM_ID)).thenReturn(gate.asMono());
            cleanup.cleanupRoom(ROOM_ID, notificationFailure ? gate.asMono() : Mono.empty())
                    .subscribe().dispose();
            gate.tryEmitError(new IllegalStateException("detached failure"));
            assertEquals(0, executionCount());
            assertTrue(completed.contains("gameLock"));
            assertTrue(appender.list.stream().anyMatch(event ->
                    event.getFormattedMessage().equals(notificationFailure
                            ? "Room (17) termination notification failed; continuing cleanup"
                            : "Room (17) cleanup failed")
                            && event.getThrowableProxy() != null
                            && event.getThrowableProxy().getMessage().equals("detached failure")));
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void simultaneousRequestsStartOnlyOneCleanup() throws Exception {
        Sinks.Empty<Void> gate = Sinks.empty();
        when(installed.cleanup(ROOM_ID)).thenReturn(gate.asMono());
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var request = (java.util.concurrent.Callable<reactor.core.Disposable>) () -> {
                ready.countDown();
                assertTrue(start.await(3, TimeUnit.SECONDS));
                return cleanup.cleanupRoom(ROOM_ID).subscribe();
            };
            var first = executor.submit(request);
            var second = executor.submit(request);
            assertTrue(ready.await(3, TimeUnit.SECONDS));
            start.countDown();
            first.get(3, TimeUnit.SECONDS).dispose();
            second.get(3, TimeUnit.SECONDS).dispose();
            assertEquals(1, executionCount());
            assertEquals(1, gate.currentSubscriberCount());
            verify(state).cleanup(ROOM_ID);
            assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
            assertEquals(0, executionCount());
        } finally {
            cleanup.shutdown();
        }
    }

    private Mono<Void> done(String resource) {
        return Mono.fromRunnable(() -> completed.add(resource));
    }
}
