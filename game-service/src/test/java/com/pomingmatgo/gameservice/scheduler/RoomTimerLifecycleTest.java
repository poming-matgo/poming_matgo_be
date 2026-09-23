package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.Disposable;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class RoomTimerLifecycleTest {
    private static final long ROOM_ID = 950_001L;
    private final RoomTimerLifecycle lifecycle = new RoomTimerLifecycle();
    private final AutoPlayScheduler scheduler = new AutoPlayScheduler(lifecycle,
            mock(InFlightManager.class), mock(GameService.class), mock(TurnFlowService.class));

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    void absentAndClosedRoomsRejectRegistrationWithoutLeavingEntries() {
        schedule(scheduler);
        assertTrue(timers().isEmpty());
        TurnScheduler absent = lifecycle.bind(ROOM_ID, scheduler);
        lifecycle.open(ROOM_ID);
        schedule(absent);
        assertTrue(timers().isEmpty(), "존재하지 않을 때 잡은 수명은 생성 후에도 유효해지지 않는다");
        closeRoom();
        schedule(scheduler);
        assertTrue(timers().isEmpty());
        assertTrue(rooms().isEmpty(), "종료된 방 ID를 tombstone으로 보관하지 않는다");
    }

    @Test
    void oldLifetimeCannotReplaceOrCancelRecreatedRoomsTimer() {
        lifecycle.open(ROOM_ID);
        TurnScheduler old = lifecycle.bind(ROOM_ID, scheduler);
        schedule(old);
        Disposable previous = timer();
        closeRoom();
        assertTrue(previous.isDisposed());
        lifecycle.open(ROOM_ID);
        schedule(lifecycle.bind(ROOM_ID, scheduler));
        Disposable current = timer();

        schedule(old);
        old.cancelAutoPlay(ROOM_ID);
        schedule(lifecycle.bind(ROOM_ID, old));
        assertSame(current, timer());
        assertFalse(current.isDisposed());
    }

    @Test
    void ordinaryCancellationKeepsRoomOpenForNextStep() {
        lifecycle.open(ROOM_ID);
        TurnScheduler bound = lifecycle.bind(ROOM_ID, scheduler);
        schedule(bound);
        Disposable previous = timer();
        bound.cancelAutoPlay(ROOM_ID);
        assertTrue(previous.isDisposed());
        schedule(bound);
        assertFalse(timer().isDisposed());
    }

    @Test
    void registrationAndRoomCleanupAreAtomic() throws Exception {
        try (var executor = Executors.newFixedThreadPool(2)) {
            for (int attempt = 0; attempt < 32; attempt++) {
                lifecycle.open(ROOM_ID);
                TurnScheduler bound = lifecycle.bind(ROOM_ID, scheduler);
                CountDownLatch start = new CountDownLatch(1);
                var registration = executor.submit(() -> {
                    assertTrue(start.await(3, TimeUnit.SECONDS));
                    schedule(bound);
                    return null;
                });
                var cleanup = executor.submit(() -> {
                    assertTrue(start.await(3, TimeUnit.SECONDS));
                    closeRoom();
                    return null;
                });
                start.countDown();
                registration.get(3, TimeUnit.SECONDS);
                cleanup.get(3, TimeUnit.SECONDS);
                assertTrue(timers().isEmpty());
                assertTrue(rooms().isEmpty());
            }
        }
    }

    private void closeRoom() {
        scheduler.onRoomCleanedUp(new RoomCleanedUpEvent(ROOM_ID));
    }

    private void schedule(TurnScheduler target) {
        target.scheduleAutoPlay(ROOM_ID, 1, 1, Player.PLAYER_1,
                System.nanoTime() + TimeUnit.MINUTES.toNanos(1), GamePhase.IN_PROGRESS);
    }

    private Map<?, ?> timers() {
        return (Map<?, ?>) ReflectionTestUtils.getField(scheduler, "scheduled");
    }

    private Map<?, ?> rooms() {
        return (Map<?, ?>) ReflectionTestUtils.getField(lifecycle, "rooms");
    }

    private Disposable timer() {
        return (Disposable) ReflectionTestUtils.getField(timers().get(ROOM_ID), "task");
    }
}
