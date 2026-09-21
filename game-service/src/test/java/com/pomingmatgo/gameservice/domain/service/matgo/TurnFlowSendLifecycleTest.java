package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import com.pomingmatgo.gameservice.scheduler.AutoPlayScheduler;
import com.pomingmatgo.gameservice.scheduler.TurnScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// 현재 결함을 기록하는 특성화 테스트다. 송신/실행 계약을 개선할 때 기대값도 함께 변경한다.
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@ActiveProfiles("in-memory")
@DisplayName("송신 완료 지연과 취소에 따른 턴 후처리 기준선")
class TurnFlowSendLifecycleTest {
    private static final long ROOM_ID = 930_001L;
    private static final long OTHER_ROOM_ID = 930_002L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    @Autowired TurnFlowService turnFlowService;
    @Autowired GamePlayService gamePlayService;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired InstalledCardRepository installedCardRepository;
    @Autowired SessionManager sessionManager;
    @Autowired RoomCleanupService roomCleanupService;
    @Autowired AutoPlayScheduler autoPlayScheduler;

    private TurnScheduler scheduler;
    private SessionProbe slow;
    private SessionProbe opponent;

    @BeforeEach
    void setUp() {
        assertTrue(AopUtils.isAopProxy(gamePlayService), "실제 게임 락 프록시를 통과해야 한다");
        scheduler = mock(TurnScheduler.class);
        slow = new SessionProbe("slow", true);
        opponent = new SessionProbe("opponent", false);
        seedRoom(ROOM_ID);
        sessionManager.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, slow.session).block(TIMEOUT);
        sessionManager.addPlayer(ROOM_ID, Player.PLAYER_2, 2L, opponent.session).block(TIMEOUT);
    }

    @AfterEach
    void cleanup() {
        for (long roomId : List.of(ROOM_ID, OTHER_ROOM_ID)) {
            roomCleanupService.cleanupRoomData(roomId).block(TIMEOUT);
            sessionManager.removeRoom(roomId).block(TIMEOUT);
        }
    }

    @Test
    @DisplayName("한 세션의 턴 안내가 지연되면 상태는 저장되지만 타이머 등록은 송신 완료까지 대기한다")
    void delayedSendDefersTimerButNotOpponentOrOtherRoom() {
        SessionProbe other = new SessionProbe("other-room", false);
        seedRoom(OTHER_ROOM_ID);
        sessionManager.addPlayer(OTHER_ROOM_ID, Player.PLAYER_1, 3L, other.session).block(TIMEOUT);

        StepVerifier.create(submit(ROOM_ID))
                .then(() -> {
                    assertWaitingForSend();
                    assertEquals(1, opponent.turnCompleted.get(), "상대방 송신은 완료돼야 한다");
                    submit(OTHER_ROOM_ID).block(TIMEOUT);
                    assertEquals(1, other.turnCompleted.get(), "다른 방 송신도 완료돼야 한다");
                    verifyScheduled(OTHER_ROOM_ID);
                    assertEquals(Sinks.EmitResult.OK, slow.completion.tryEmitEmpty());
                })
                .expectComplete()
                .verify(TIMEOUT);

        assertEquals(1, slow.turnCompleted.get());
        assertEquals(0, slow.turnCancelled.get());
        verifyScheduled(ROOM_ID);
    }

    @Test
    @DisplayName("턴 안내 도중 요청 취소 시 저장된 상태는 남지만 다음 타이머 등록은 누락된다")
    void cancellationLeavesSavedStateWithoutNextTimer() {
        StepVerifier.create(submit(ROOM_ID))
                .then(this::assertWaitingForSend)
                .thenCancel()
                .verify(TIMEOUT);

        assertEquals(1, slow.turnCancelled.get());
        assertEquals(0, slow.turnCompleted.get());
        assertNextTurnSaved();
        // 취소 후 완료 신호가 와도 후처리가 다시 구독되지 않는다.
        assertEquals(Sinks.EmitResult.OK, slow.completion.tryEmitEmpty());
        verify(scheduler, never()).scheduleAutoPlay(eq(ROOM_ID), anyInt(), anyInt(),
                any(Player.class), anyLong(), any(GamePhase.class));

        // 같은 방의 다음 유효 액션으로 락 누수와 타이머 누락을 구분한다.
        turnFlowService.processNormalSubmit(ROOM_ID, Player.PLAYER_2, 0,
                () -> scheduler.cancelAutoPlay(ROOM_ID), scheduler).block(TIMEOUT);
        GameState after = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        assertNotNull(after);
        assertEquals(2, after.getRound());
        assertEquals(Player.PLAYER_1, after.getCurrentPlayer());
    }

    @Test
    @DisplayName("송신 대기 중 방 데이터와 세션 매핑은 정리되지만 기존 송신 구독은 별도 취소가 필요하다")
    void cleanupDoesNotOwnPendingSendSubscription() {
        StepVerifier.create(submit(ROOM_ID))
                .then(() -> {
                    assertWaitingForSend();
                    roomCleanupService.cleanupRoomData(ROOM_ID).block(TIMEOUT);
                    sessionManager.removeRoom(ROOM_ID).block(TIMEOUT);
                    assertNull(gameStateRepository.findById(ROOM_ID).block(TIMEOUT));
                    assertTrue(sessionManager.getAllUser(ROOM_ID).isEmpty());
                    assertNull(sessionManager.getPlayerContext(slow.session).block(TIMEOUT));
                    assertNull(sessionManager.getPlayerContext(opponent.session).block(TIMEOUT));
                    assertEquals(0, slow.turnCompleted.get());
                    assertEquals(0, slow.turnCancelled.get(), "정리 자체는 송신 구독을 취소하지 않는다");
                })
                .thenCancel()
                .verify(TIMEOUT);

        assertEquals(1, slow.turnCancelled.get());
        verify(scheduler, never()).scheduleAutoPlay(eq(ROOM_ID), anyInt(), anyInt(),
                any(Player.class), anyLong(), any(GamePhase.class));
    }

    @ParameterizedTest(name = "송신 실패={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("방 정리 후 송신 정상 완료 또는 오류 복구가 실제 타이머를 다시 만들고 발사 후에도 엔트리를 남긴다")
    void lateSendTerminationRecreatesTimerAfterCleanup(boolean failSend) {
        StepVerifier.withVirtualTime(() -> submit(ROOM_ID, autoPlayScheduler)
                        .then(Mono.delay(Duration.ofSeconds(13))))
                .then(() -> {
                    assertEquals(1, slow.turnStarted.get());
                    assertEquals(1, opponent.turnCompleted.get());
                    assertNextTurnSaved();
                    assertFalse(scheduledTimers().containsKey(ROOM_ID));

                    cleanupRoom();
                    assertRoomRemoved();
                    assertFalse(scheduledTimers().containsKey(ROOM_ID));
                    assertEquals(0, slow.turnCancelled.get());

                    Sinks.EmitResult result = failSend
                            ? slow.completion.tryEmitError(new IllegalStateException("controlled send failure"))
                            : slow.completion.tryEmitEmpty();
                    assertEquals(Sinks.EmitResult.OK, result);
                    assertEquals(failSend ? 0 : 1, slow.turnCompleted.get());
                    assertEquals(failSend ? 1 : 0, slow.turnFailed.get());
                    assertFalse(scheduledTask().isDisposed(), "삭제된 방에 대기 타이머가 다시 생성된다");
                    assertRoomRemoved();
                })
                // nanoTime deadline은 그대로 두고 Reactor delay만 가상 시간으로 발사한다.
                .thenAwait(Duration.ofSeconds(13))
                .expectNext(0L)
                .then(() -> {
                    assertRoomRemoved();
                    assertTrue(scheduledTask().isDisposed(), "타이머는 발사됐지만 맵 엔트리는 남는다");
                })
                .expectComplete()
                .verify(TIMEOUT);

        cleanupRoom();
        assertFalse(scheduledTimers().containsKey(ROOM_ID), "재정리 이벤트는 잔존 엔트리를 제거한다");
    }

    @Test
    @DisplayName("송신 완료 후 방을 정리하면 실제 대기 타이머가 취소되고 엔트리도 제거된다")
    void cleanupAfterSendCompletionRemovesRealTimer() {
        Disposable[] pendingTask = new Disposable[1];
        StepVerifier.withVirtualTime(() -> submit(ROOM_ID, autoPlayScheduler))
                .then(() -> {
                    assertEquals(1, slow.turnStarted.get());
                    assertNextTurnSaved();
                    assertEquals(Sinks.EmitResult.OK, slow.completion.tryEmitEmpty());
                    pendingTask[0] = scheduledTask();
                    assertFalse(pendingTask[0].isDisposed());
                    cleanupRoom();
                    assertTrue(pendingTask[0].isDisposed());
                    assertFalse(scheduledTimers().containsKey(ROOM_ID));
                    assertRoomRemoved();
                })
                .expectComplete()
                .verify(TIMEOUT);
    }

    private void cleanupRoom() {
        roomCleanupService.cleanupRoomData(ROOM_ID).block(TIMEOUT);
        sessionManager.removeRoom(ROOM_ID).block(TIMEOUT);
    }

    private void assertRoomRemoved() {
        assertNull(gameStateRepository.findById(ROOM_ID).block(TIMEOUT));
        assertTrue(sessionManager.getAllUser(ROOM_ID).isEmpty());
        assertNull(sessionManager.getPlayerContext(slow.session).block(TIMEOUT));
        assertNull(sessionManager.getPlayerContext(opponent.session).block(TIMEOUT));
        assertTrue(installedCardRepository.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).isEmpty());
        assertTrue(installedCardRepository.getPlayerCards(ROOM_ID, Player.PLAYER_2).block(TIMEOUT).isEmpty());
    }

    // 완료된 Disposable과 맵 엔트리의 잔존을 구분하기 위한 테스트 전용 관측이다.
    private Map<?, ?> scheduledTimers() {
        return (Map<?, ?>) ReflectionTestUtils.getField(autoPlayScheduler, "scheduled");
    }

    private Disposable scheduledTask() {
        Object scheduled = scheduledTimers().get(ROOM_ID);
        assertNotNull(scheduled, "해당 방의 타이머 엔트리가 있어야 한다");
        return (Disposable) ReflectionTestUtils.getField(scheduled, "task");
    }

    private Mono<Void> submit(long roomId) {
        return submit(roomId, scheduler);
    }

    private Mono<Void> submit(long roomId, TurnScheduler targetScheduler) {
        return turnFlowService.processNormalSubmit(roomId, Player.PLAYER_1, 0,
                () -> targetScheduler.cancelAutoPlay(roomId), targetScheduler);
    }

    private void assertWaitingForSend() {
        assertEquals(1, slow.turnStarted.get(), "턴 안내 송신 구독까지 도달해야 한다");
        assertEquals(0, slow.turnCompleted.get());
        assertEquals(0, slow.turnCancelled.get());
        assertNextTurnSaved();
        verify(scheduler).cancelAutoPlay(ROOM_ID);
        verify(scheduler, never()).scheduleAutoPlay(eq(ROOM_ID), anyInt(), anyInt(),
                any(Player.class), anyLong(), any(GamePhase.class));
    }

    private void assertNextTurnSaved() {
        GameState state = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        assertNotNull(state);
        assertEquals(GamePhase.IN_PROGRESS, state.getPhase());
        assertEquals(1, state.getRound());
        assertEquals(2, state.getCurrentTurn());
        assertEquals(Player.PLAYER_2, state.getCurrentPlayer());
        assertTrue(installedCardRepository.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).isEmpty());
    }

    private void verifyScheduled(long roomId) {
        verify(scheduler).scheduleAutoPlay(eq(roomId), eq(1), eq(2),
                eq(Player.PLAYER_2), anyLong(), eq(GamePhase.IN_PROGRESS));
    }

    private void seedRoom(long roomId) {
        gameStateRepository.create(GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS).build()).block(TIMEOUT);
        installedCardRepository.savePlayerCards(List.of(Card.JAN_3), roomId, Player.PLAYER_1).block(TIMEOUT);
        installedCardRepository.savePlayerCards(List.of(Card.FEB_3), roomId, Player.PLAYER_2).block(TIMEOUT);
        installedCardRepository.saveHiddenCard(List.of(Card.MAR_1, Card.APR_1), roomId).block(TIMEOUT);
    }

    // 실제 직렬화/브로드캐스트는 유지하고 WebSocket send 완료만 제어한다.
    private static class SessionProbe {
        final WebSocketSession session = mock(WebSocketSession.class);
        final Sinks.Empty<Void> completion = Sinks.empty();
        final AtomicInteger turnStarted = new AtomicInteger();
        final AtomicInteger turnCompleted = new AtomicInteger();
        final AtomicInteger turnCancelled = new AtomicInteger();
        final AtomicInteger turnFailed = new AtomicInteger();

        SessionProbe(String id, boolean delayTurn) {
            when(session.getId()).thenReturn(id);
            when(session.isOpen()).thenReturn(true);
            when(session.textMessage(anyString())).thenAnswer(invocation ->
                    new WebSocketMessage(WebSocketMessage.Type.TEXT,
                            DefaultDataBufferFactory.sharedInstance.wrap(
                                    invocation.<String>getArgument(0).getBytes(StandardCharsets.UTF_8))));
            when(session.send(any())).thenAnswer(invocation -> {
                Publisher<WebSocketMessage> messages = invocation.getArgument(0);
                return Flux.from(messages).concatMap(message -> {
                    boolean turn = message.getPayloadAsText().contains("\"status\":\"ANNOUNCE_TURN_INFORMATION\"");
                    message.release();
                    if (!turn) return Mono.<Void>empty();
                    return Mono.defer(() -> {
                        turnStarted.incrementAndGet();
                        return (delayTurn ? completion.asMono() : Mono.<Void>empty())
                                .doOnSuccess(ignored -> turnCompleted.incrementAndGet())
                                .doOnError(ignored -> turnFailed.incrementAndGet())
                                .doOnCancel(turnCancelled::incrementAndGet);
                    });
                }).then();
            });
        }
    }
}
