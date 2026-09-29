package com.pomingmatgo.gameservice.infrastructure.scheduler;

import com.pomingmatgo.gameservice.application.game.GameActionSource;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.application.game.GameService;
import com.pomingmatgo.gameservice.application.game.TurnFlowService;
import com.pomingmatgo.gameservice.infrastructure.lock.InFlightManager;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AutoPlaySubscriptionLifecycleTest {
    private static final long ROOM_ID = 940_001L;
    private final InFlightManager inFlight = mock(InFlightManager.class);
    private final GameService gameService = mock(GameService.class);
    private final TurnFlowService turnFlow = mock(TurnFlowService.class);
    private final RoomTimerLifecycle lifecycle = new RoomTimerLifecycle();
    private final AutoPlayScheduler scheduler = new AutoPlayScheduler(lifecycle, inFlight, gameService, turnFlow);
    private VirtualTimeScheduler clock;

    @BeforeEach
    void setUp() {
        clock = VirtualTimeScheduler.getOrSet();
        when(gameService.findGameState(anyLong())).thenAnswer(call -> Mono.just(GameState.builder()
                .roomId(call.getArgument(0)).round(1).currentTurn(1).leadingPlayer(1)
                .phase(GamePhase.IN_PROGRESS).build()));
        when(inFlight.isSet(anyString())).thenReturn(Mono.just(false));
        when(inFlight.trySetFlag(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        when(inFlight.deleteFlag(anyString(), anyString())).thenReturn(Mono.empty());
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
        VirtualTimeScheduler.reset();
    }

    @Test
    void executionIsTrackedUntilCompletionAndTimerCancellationDoesNotCancelIt() {
        Sinks.Empty<Void> completion = Sinks.empty();
        AtomicBoolean cancelled = new AtomicBoolean();
        stubAction(completion.asMono().doOnCancel(() -> cancelled.set(true)));
        schedule(ROOM_ID);
        assertEquals(0, running().size());
        fire();
        assertEquals(1, running().size());

        scheduler.cancelAutoPlay(ROOM_ID);
        scheduler.onRoomCleanedUp(new RoomCleanedUpEvent(ROOM_ID));
        assertFalse(cancelled.get(), "대기 취소와 방 정리는 발사된 필수 처리를 끊지 않는다");
        assertEquals(1, running().size());
        assertEquals(Sinks.EmitResult.OK, completion.tryEmitEmpty());
        assertEquals(0, running().size());
        verify(inFlight).deleteFlag(eq(InFlightManager.autoplayKey(ROOM_ID, 1)), anyString());
    }

    @Test
    void busyUserRequestRearmsTimerAndReleasesWaitingExecution() {
        when(inFlight.isSet(anyString())).thenReturn(Mono.just(true));
        stubAction(Mono.empty());
        schedule(ROOM_ID);
        fire();

        assertEquals(0, running().size());
        assertFalse(timerTask(ROOM_ID).isDisposed());
        verifyNoInteractions(turnFlow);
        verify(inFlight, never()).trySetFlag(anyString(), anyString(), any(Duration.class));

        when(inFlight.isSet(anyString())).thenReturn(Mono.just(false));
        clock.advanceTimeBy(Duration.ofSeconds(1));
        verify(turnFlow).processNormalSubmit(eq(ROOM_ID), eq(Player.PLAYER_1), eq(0),
                eq(GameActionSource.AUTOPLAY), any(TurnScheduler.class));
        assertEquals(0, running().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancellingBusyUserRetryPreventsFurtherReadsEvenAfterRoomRecreation(boolean recreate) {
        when(inFlight.isSet(anyString())).thenReturn(Mono.just(true));
        schedule(ROOM_ID);
        fire();
        scheduler.onRoomCleanedUp(new RoomCleanedUpEvent(ROOM_ID));
        if (recreate) lifecycle.open(ROOM_ID);
        clearInvocations(gameService, inFlight);

        clock.advanceTimeBy(Duration.ofSeconds(3));

        assertEquals(0, running().size());
        assertTrue(timers().isEmpty());
        verifyNoInteractions(gameService, inFlight, turnFlow);
    }

    @Test
    void lateBusyUserCheckDoesNotReplaceNewTimerForTheSameStep() {
        Sinks.One<Boolean> busy = Sinks.one();
        when(inFlight.isSet(anyString())).thenReturn(busy.asMono());
        schedule(ROOM_ID);
        fire();
        schedule(ROOM_ID);
        Disposable replacement = timerTask(ROOM_ID);

        assertEquals(Sinks.EmitResult.OK, busy.tryEmitValue(true));

        assertSame(replacement, timerTask(ROOM_ID));
        assertFalse(replacement.isDisposed());
        assertEquals(0, running().size());
        verifyNoInteractions(turnFlow);
    }

    @Test
    void lockContentionReleasesExecutionAndFlagBeforeRetrying() {
        Sinks.Empty<Void> firstAttempt = Sinks.empty();
        stubAction(firstAttempt.asMono());
        schedule(ROOM_ID);
        fire();
        stubAction(Mono.empty());
        assertEquals(Sinks.EmitResult.OK, firstAttempt.tryEmitError(
                new WebSocketBusinessException(WebSocketErrorCode.TRY_AGAIN)));
        assertEquals(0, running().size());
        assertFalse(timerTask(ROOM_ID).isDisposed());
        verify(inFlight).deleteFlag(eq(InFlightManager.autoplayKey(ROOM_ID, 1)), anyString());

        clock.advanceTimeBy(Duration.ofSeconds(1));
        verify(turnFlow, times(2)).processNormalSubmit(eq(ROOM_ID), eq(Player.PLAYER_1), eq(0),
                eq(GameActionSource.AUTOPLAY), any(TurnScheduler.class));
        verify(inFlight, times(2)).deleteFlag(eq(InFlightManager.autoplayKey(ROOM_ID, 1)), anyString());
        assertEquals(0, running().size());
    }

    @Test
    void lateContentionDoesNotReplaceNewTimerForTheSameStep() {
        Sinks.Empty<Void> action = Sinks.empty();
        stubAction(action.asMono());
        schedule(ROOM_ID);
        fire();
        schedule(ROOM_ID);
        Disposable replacement = timerTask(ROOM_ID);
        assertEquals(Sinks.EmitResult.OK, action.tryEmitError(
                new WebSocketBusinessException(WebSocketErrorCode.TRY_AGAIN)));
        assertSame(replacement, timerTask(ROOM_ID));
        assertFalse(replacement.isDisposed());
        assertEquals(0, running().size());
    }

    @Test
    void retryRechecksCurrentStepBeforePlaying() {
        stubAction(Mono.error(new WebSocketBusinessException(WebSocketErrorCode.TRY_AGAIN)));
        schedule(ROOM_ID);
        fire();
        when(gameService.findGameState(ROOM_ID)).thenReturn(Mono.just(GameState.builder()
                .roomId(ROOM_ID).round(1).currentTurn(2).leadingPlayer(1).phase(GamePhase.IN_PROGRESS).build()));
        clock.advanceTimeBy(Duration.ofSeconds(1));
        verify(turnFlow, times(1)).processNormalSubmit(eq(ROOM_ID), eq(Player.PLAYER_1), eq(0),
                eq(GameActionSource.AUTOPLAY), any(TurnScheduler.class));
        assertEquals(0, running().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void lateContentionCannotRearmClosedOrRecreatedRoom(boolean recreate) {
        Sinks.Empty<Void> action = Sinks.empty();
        stubAction(action.asMono());
        schedule(ROOM_ID);
        fire();
        scheduler.onRoomCleanedUp(new RoomCleanedUpEvent(ROOM_ID));
        Disposable replacement = null;
        if (recreate) {
            lifecycle.open(ROOM_ID);
            schedule(ROOM_ID);
            replacement = timerTask(ROOM_ID);
        }
        assertEquals(Sinks.EmitResult.OK, action.tryEmitError(
                new WebSocketBusinessException(WebSocketErrorCode.TRY_AGAIN)));
        assertEquals(0, running().size());
        if (recreate) {
            assertSame(replacement, timerTask(ROOM_ID));
            assertFalse(replacement.isDisposed());
        } else {
            assertTrue(timers().isEmpty());
        }
    }

    @Test
    void synchronousCompletionDoesNotLeaveTrackedExecution() {
        stubAction(Mono.empty());
        schedule(ROOM_ID);
        fire();
        verify(turnFlow).processNormalSubmit(eq(ROOM_ID), eq(Player.PLAYER_1), eq(0), eq(GameActionSource.AUTOPLAY), any(TurnScheduler.class));
        assertEquals(0, running().size());
    }

    @Test
    void runningExecutionKeepsOriginalLifetimeAfterRoomRecreation() {
        Sinks.Empty<Void> completion = Sinks.empty();
        when(turnFlow.processNormalSubmit(anyLong(), any(Player.class), anyInt(), eq(GameActionSource.AUTOPLAY), any(TurnScheduler.class)))
                .thenAnswer(call -> {
                    TurnScheduler bound = call.getArgument(4);
                    return completion.asMono().then(Mono.fromRunnable(() ->
                            bound.scheduleAutoPlay(ROOM_ID, 2, 1, Player.PLAYER_1,
                                    System.nanoTime(), GamePhase.IN_PROGRESS)));
                });
        schedule(ROOM_ID);
        fire();
        assertEquals(1, running().size());
        scheduler.onRoomCleanedUp(new RoomCleanedUpEvent(ROOM_ID));
        lifecycle.open(ROOM_ID);
        schedule(ROOM_ID);
        Disposable current = timerTask(ROOM_ID);
        assertEquals(Sinks.EmitResult.OK, completion.tryEmitEmpty());
        assertEquals(0, running().size());
        assertSame(current, timerTask(ROOM_ID));
        assertFalse(current.isDisposed());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void publisherCreationAndAsyncErrorsAreObservedAndRemoved(boolean synchronous) {
        IllegalStateException failure = new IllegalStateException("controlled autoplay failure");
        Sinks.Empty<GameState> result = Sinks.empty();
        if (synchronous) {
            when(gameService.findGameState(ROOM_ID)).thenThrow(failure);
        } else {
            when(gameService.findGameState(ROOM_ID)).thenReturn(result.asMono());
        }
        Logger logger = (Logger) LoggerFactory.getLogger(AutoPlayScheduler.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            schedule(ROOM_ID);
            fire();
            if (!synchronous) {
                assertEquals(1, running().size());
                assertEquals(Sinks.EmitResult.OK, result.tryEmitError(failure));
            }
            assertEquals(0, running().size());
            assertTrue(appender.list.stream().anyMatch(event ->
                    event.getFormattedMessage().contains(Long.toString(ROOM_ID))
                            && event.getThrowableProxy() != null
                            && event.getThrowableProxy().getMessage().equals(failure.getMessage())));
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void shutdownCancelsRunningAndWaitingTasksAndRejectsNewTimers() {
        AtomicInteger cancelled = new AtomicInteger();
        stubAction(Mono.<Void>never().doOnCancel(cancelled::incrementAndGet));
        schedule(ROOM_ID);
        schedule(ROOM_ID + 3);
        fire();
        assertEquals(2, running().size());
        schedule(ROOM_ID + 1);
        Disposable pending = timerTask(ROOM_ID + 1);
        assertFalse(pending.isDisposed());

        scheduler.shutdown();
        scheduler.shutdown();
        assertEquals(2, cancelled.get());
        assertTrue(pending.isDisposed());
        assertEquals(0, running().size());
        assertTrue(timers().isEmpty());
        verify(inFlight).deleteFlag(eq(InFlightManager.autoplayKey(ROOM_ID, 1)), anyString());

        schedule(ROOM_ID + 2);
        fire();
        assertTrue(timers().isEmpty());
        verify(gameService, never()).findGameState(ROOM_ID + 1);
        verify(gameService, never()).findGameState(ROOM_ID + 2);
    }

    @Test
    void concurrentRegistrationAndShutdownLeaveNoTimers() throws Exception {
        try (var executor = Executors.newFixedThreadPool(2)) {
            for (int attempt = 0; attempt < 32; attempt++) {
                RoomTimerLifecycle targetLifecycle = new RoomTimerLifecycle();
                targetLifecycle.open(ROOM_ID);
                AutoPlayScheduler target = new AutoPlayScheduler(targetLifecycle, inFlight, gameService, turnFlow);
                CountDownLatch start = new CountDownLatch(1);
                var registration = executor.submit(() -> {
                    assertTrue(start.await(3, TimeUnit.SECONDS));
                    target.scheduleAutoPlay(ROOM_ID, 1, 1, Player.PLAYER_1,
                            System.nanoTime(), GamePhase.IN_PROGRESS);
                    return null;
                });
                var shutdown = executor.submit(() -> {
                    assertTrue(start.await(3, TimeUnit.SECONDS));
                    target.shutdown();
                    return null;
                });
                start.countDown();
                try {
                    registration.get(3, TimeUnit.SECONDS);
                    shutdown.get(3, TimeUnit.SECONDS);
                    assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(target, "scheduled")).isEmpty());
                } finally {
                    target.shutdown();
                }
            }
        }
        fire();
        verifyNoInteractions(gameService, turnFlow);
    }

    @Test
    void shutdownDuringPublisherCreationCancelsLateSubscription() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean cancelled = new AtomicBoolean();
        when(gameService.findGameState(ROOM_ID)).thenAnswer(call -> {
            entered.countDown();
            assertTrue(release.await(3, TimeUnit.SECONDS));
            return Mono.<GameState>never().doOnCancel(() -> cancelled.set(true));
        });
        schedule(ROOM_ID);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var firing = executor.submit(this::fire);
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                assertEquals(1, running().size());
                scheduler.shutdown();
                assertEquals(0, running().size());
            } finally {
                release.countDown();
            }
            firing.get(3, TimeUnit.SECONDS);
        }
        assertTrue(cancelled.get());
        assertTrue(timers().isEmpty());
        verifyNoInteractions(turnFlow);
    }

    private void stubAction(Mono<Void> action) {
        when(turnFlow.processNormalSubmit(anyLong(), any(Player.class), anyInt(), eq(GameActionSource.AUTOPLAY), any(TurnScheduler.class)))
                .thenReturn(action);
    }

    private void schedule(long roomId) {
        if (!lifecycle.isOpen(roomId)) lifecycle.open(roomId);
        scheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.IN_PROGRESS);
    }

    private void fire() {
        clock.advanceTimeBy(Duration.ofMillis(100));
    }

    private Disposable.Composite running() {
        return (Disposable.Composite) ReflectionTestUtils.getField(scheduler, "runningAutoPlays");
    }

    private Map<?, ?> timers() {
        return (Map<?, ?>) ReflectionTestUtils.getField(scheduler, "scheduled");
    }

    private Disposable timerTask(long roomId) {
        return (Disposable) ReflectionTestUtils.getField(timers().get(roomId), "task");
    }
}
