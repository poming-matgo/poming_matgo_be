package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.global.lock.RoomLockManager;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RoomCleanupFailureTest {
    private static final long ROOM_ID = 17L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final GameStateRepository state = mock(GameStateRepository.class);
    private final InstalledCardRepository installed = mock(InstalledCardRepository.class);
    private final AcquiredCardRepository acquired = mock(AcquiredCardRepository.class);
    private final LeadingPlayerRepository leader = mock(LeadingPlayerRepository.class);
    private final RoomLockManager roomLock = mock(RoomLockManager.class);
    private final GameLockCleaner gameLock = mock(GameLockCleaner.class);
    private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
    private final List<String> completed = new ArrayList<>();
    private final RoomCleanupService cleanup = new RoomCleanupService(
            state, installed, acquired, leader, roomLock, gameLock, events, new SessionManager());

    @BeforeEach
    void setUp() {
        when(state.cleanup(ROOM_ID)).thenReturn(done("state"));
        when(installed.cleanup(ROOM_ID)).thenReturn(done("installed"));
        when(acquired.cleanup(ROOM_ID)).thenReturn(done("acquired"));
        when(leader.cleanup(ROOM_ID)).thenReturn(done("leader"));
        when(roomLock.cleanup(ROOM_ID)).thenReturn(done("roomLock"));
        when(gameLock.cleanup(ROOM_ID)).thenReturn(done("gameLock"));
        doAnswer(invocation -> {
            completed.add("event");
            return null;
        }).when(events).publishEvent(new RoomCleanedUpEvent(ROOM_ID));
    }

    @Test
    void successfulCleanupIsLazyAndCompletesAllResources() {
        clearInvocations(state, installed, acquired, leader, roomLock, gameLock, events);
        Mono<Void> result = cleanup.cleanupRoomData(ROOM_ID);
        verifyNoInteractions(state, installed, acquired, leader, roomLock, gameLock, events);
        StepVerifier.create(result).expectComplete().verify(TIMEOUT);
        assertEquals(List.of("state", "installed", "acquired", "leader", "roomLock", "gameLock", "event"), completed);
    }

    @Test
    void firstFailureStillCleansOtherResourcesAndPublishesEvent() {
        RuntimeException failure = new IllegalStateException("state cleanup failed");
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(failure));
        StepVerifier.create(cleanup.cleanupRoomData(ROOM_ID))
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        assertEquals(List.of("installed", "acquired", "leader", "roomLock", "gameLock", "event"), completed);
    }

    @Test
    void synchronousFailureDoesNotPreventOtherCleanup() {
        RuntimeException failure = new IllegalStateException("cleanup construction failed");
        when(state.cleanup(ROOM_ID)).thenThrow(failure);
        StepVerifier.create(cleanup.cleanupRoomData(ROOM_ID))
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        assertEquals(List.of("installed", "acquired", "leader", "roomLock", "gameLock", "event"), completed);
    }

    @Test
    void multipleFailuresArePreserved() {
        RuntimeException first = new IllegalStateException("state cleanup failed");
        RuntimeException second = new IllegalStateException("lock cleanup failed");
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(first));
        when(gameLock.cleanup(ROOM_ID)).thenReturn(Mono.error(second));
        StepVerifier.create(cleanup.cleanupRoomData(ROOM_ID))
                .expectErrorSatisfies(error -> assertEquals(
                        List.of(first, second), Exceptions.unwrapMultipleExcludingTracebacks(error)))
                .verify(TIMEOUT);
        assertEquals(List.of("installed", "acquired", "leader", "roomLock", "event"), completed);
    }

    @Test
    void errorWaitsForPendingCleanupWithoutCancellingIt() {
        Sinks.Empty<Void> pending = Sinks.empty();
        RuntimeException failure = new IllegalStateException("state cleanup failed");
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(failure));
        when(installed.cleanup(ROOM_ID)).thenReturn(pending.asMono().then(done("installed")));
        StepVerifier.create(cleanup.cleanupRoomData(ROOM_ID))
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
        StepVerifier.create(cleanup.cleanupRoomData(ROOM_ID))
                .then(() -> {
                    assertEquals(1, pending.currentSubscriberCount());
                    assertEquals(Sinks.EmitResult.OK, pending.tryEmitEmpty());
                })
                .expectErrorSatisfies(error -> {
                    assertSame(failure, error);
                    assertTrue(completed.contains("state"));
                }).verify(TIMEOUT);
    }

    private Mono<Void> done(String resource) {
        return Mono.fromRunnable(() -> completed.add(resource));
    }
}
