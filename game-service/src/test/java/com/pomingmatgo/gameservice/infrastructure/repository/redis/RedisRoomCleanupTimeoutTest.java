package com.pomingmatgo.gameservice.infrastructure.repository.redis;

import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.infrastructure.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

// Redis 응답 Publisher를 제어하는 공통 정리 계약 검증이며 실제 Redis 연결 테스트는 아니다.
class RedisRoomCleanupTimeoutTest {
    private static final long ROOM_ID = 17L;
    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(3);
    @SuppressWarnings("unchecked")
    private final ReactiveRedisOperations<String, GameState> stateOps = mock(ReactiveRedisOperations.class);
    @SuppressWarnings("unchecked")
    private final ReactiveRedisOperations<String, String> installedOps = mock(ReactiveRedisOperations.class);
    @SuppressWarnings("unchecked")
    private final ReactiveRedisOperations<String, String> acquiredOps = mock(ReactiveRedisOperations.class);
    @SuppressWarnings("unchecked")
    private final ReactiveRedisOperations<String, String> leaderOps = mock(ReactiveRedisOperations.class);
    private final List<ReactiveRedisOperations<String, ?>> operations =
            List.of(stateOps, installedOps, acquiredOps, leaderOps);
    private final SessionManager sessions = mock(SessionManager.class);
    private final GameLockCleaner lock = mock(GameLockCleaner.class, CALLS_REAL_METHODS);
    private final RoomCleanupService cleanup = new RoomCleanupService(
            new RedisGameStateRepository(stateOps, new RoomTimerLifecycle()),
            new RedisInstalledCardRepository(installedOps),
            new RedisAcquiredCardRepository(acquiredOps),
            new RedisLeadingPlayerRepository(leaderOps), lock,
            mock(ApplicationEventPublisher.class), sessions);

    private void successfulDeletes() {
        operations.forEach(ops -> when(ops.delete(any(String[].class))).thenReturn(Mono.just(1L)));
        when(lock.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(sessions.removeRoom(ROOM_ID)).thenReturn(Mono.empty());
    }

    @Test
    void successfulRemoteDeletesCompleteSessionCleanup() {
        successfulDeletes();
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID)).expectComplete().verify(VERIFY_TIMEOUT);
        operations.forEach(ops -> verify(ops).delete(any(String[].class)));
        verify(lock).cleanup(ROOM_ID);
        verify(sessions).removeRoom(ROOM_ID);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void remoteDeleteTimeoutPreservesOtherFailureAndStillRemovesSessions(int pendingIndex) {
        successfulDeletes();
        AtomicBoolean cancelled = new AtomicBoolean();
        var failure = new IllegalStateException("another Redis delete failed");
        when(operations.get(pendingIndex).delete(any(String[].class)))
                .thenReturn(Mono.<Long>never().doOnCancel(() -> cancelled.set(true)));
        when(operations.get((pendingIndex + 1) % operations.size()).delete(any(String[].class)))
                .thenReturn(Mono.error(failure));

        StepVerifier.withVirtualTime(() -> cleanup.cleanupRoom(ROOM_ID))
                .thenAwait(Duration.ofSeconds(29))
                .then(() -> {
                    assertFalse(cancelled.get());
                    verify(sessions, never()).removeRoom(ROOM_ID);
                })
                .thenAwait(Duration.ofSeconds(1))
                .expectErrorSatisfies(error -> {
                    var errors = Exceptions.unwrapMultipleExcludingTracebacks(error);
                    assertEquals(2, errors.size());
                    assertTrue(errors.contains(failure));
                    assertTrue(errors.stream().anyMatch(TimeoutException.class::isInstance));
                }).verify(VERIFY_TIMEOUT);

        assertTrue(cancelled.get());
        operations.forEach(ops -> verify(ops).delete(any(String[].class)));
        verify(lock).cleanup(ROOM_ID);
        verify(sessions).removeRoom(ROOM_ID);
    }
}
