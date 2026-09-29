package com.pomingmatgo.gameservice.application.game;

import com.pomingmatgo.gameservice.application.room.RoomCleanupService;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import com.pomingmatgo.gameservice.infrastructure.scheduler.AutoPlayScheduler;
import com.pomingmatgo.gameservice.infrastructure.scheduler.TurnScheduler;
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

// 송신과 무관한 타이머 등록 및 방 정리 후 재등록 차단을 검증한다.
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
    @Autowired AcquiredCardRepository acquiredCardRepository;
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
    @DisplayName("턴 안내가 지연돼도 다음 타이머와 상대방·다른 방 송신은 진행한다")
    void delayedSendDoesNotDeferTimerOrOpponentOrOtherRoom() {
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
    @DisplayName("턴 안내 도중 요청을 취소해도 저장된 상태와 다음 타이머는 유지된다")
    void cancellationPreservesSavedStateAndNextTimer() {
        StepVerifier.create(submit(ROOM_ID))
                .then(this::assertWaitingForSend)
                .thenCancel()
                .verify(TIMEOUT);

        assertEquals(1, slow.turnCancelled.get());
        assertEquals(0, slow.turnCompleted.get());
        assertNextTurnSaved();
        // 취소 후 완료 신호가 와도 후처리가 다시 구독되지 않는다.
        assertEquals(Sinks.EmitResult.OK, slow.completion.tryEmitEmpty());
        verifyScheduled(ROOM_ID);

        // 다음 유효 액션도 정상 진행한다.
        turnFlowService.processNormalSubmit(ROOM_ID, Player.PLAYER_2, 0,
                GameActionSource.USER, scheduler).block(TIMEOUT);
        GameState after = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        assertNotNull(after);
        assertEquals(2, after.getRound());
        assertEquals(Player.PLAYER_1, after.getCurrentPlayer());
    }

    @Test
    @DisplayName("송신 중 취소된 요청의 실제 다음 타이머가 발사되어 게임을 진행한다")
    void cancelledSendLeavesAnExecutableTimer() {
        StepVerifier.withVirtualTime(() -> Mono.defer(() -> {
                    Disposable request = submit(ROOM_ID, autoPlayScheduler).subscribe();
                    assertEquals(1, slow.turnStarted.get());
                    request.dispose();
                    assertEquals(1, slow.turnCancelled.get());
                    assertTrue(scheduledTimers().containsKey(ROOM_ID));
                    return Mono.delay(Duration.ofSeconds(13));
                }))
                .thenAwait(Duration.ofSeconds(13))
                .expectNext(0L)
                .then(() -> {
                    GameState state = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
                    assertEquals(2, state.getRound());
                    assertEquals(Player.PLAYER_1, state.getCurrentPlayer());
                    assertEquals(2, slow.turnStarted.get());
                    assertEquals(Sinks.EmitResult.OK, slow.completion.tryEmitEmpty());
                })
                .expectComplete().verify(TIMEOUT);
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
        verifyScheduled(ROOM_ID);
    }

    @ParameterizedTest(name = "송신 실패={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("방 정리 후 송신 정상 완료 또는 오류 복구가 타이머를 다시 만들지 않는다")
    void lateSendTerminationDoesNotRecreateTimerAfterCleanup(boolean failSend) {
        StepVerifier.withVirtualTime(() -> submit(ROOM_ID, autoPlayScheduler)
                        .then(Mono.delay(Duration.ofSeconds(13))))
                .then(() -> {
                    assertEquals(1, slow.turnStarted.get());
                    assertEquals(1, opponent.turnCompleted.get());
                    assertNextTurnSaved();
                    assertTrue(scheduledTimers().containsKey(ROOM_ID));

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
                    assertFalse(scheduledTimers().containsKey(ROOM_ID), "삭제된 방의 타이머 등록을 거부한다");
                    assertRoomRemoved();
                })
                // nanoTime deadline은 그대로 두고 Reactor delay만 가상 시간으로 발사한다.
                .thenAwait(Duration.ofSeconds(13))
                .expectNext(0L)
                .then(() -> {
                    assertRoomRemoved();
                    assertFalse(scheduledTimers().containsKey(ROOM_ID));
                })
                .expectComplete()
                .verify(TIMEOUT);

        cleanupRoom();
        assertFalse(scheduledTimers().containsKey(ROOM_ID), "반복 정리 후에도 엔트리가 없어야 한다");
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

    @ParameterizedTest(name = "송신 실패={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("같은 ID의 방이 다시 생성돼도 이전 송신 후처리는 새 타이머를 덮어쓰지 않는다")
    void lateSendCannotReplaceRecreatedRoomsTimer(boolean failSend) {
        StepVerifier.create(submit(ROOM_ID, autoPlayScheduler))
                .then(() -> {
                    assertEquals(1, slow.turnStarted.get());
                    cleanupRoom();
                    seedRoom(ROOM_ID);
                    autoPlayScheduler.scheduleAutoPlay(ROOM_ID, 1, 1, Player.PLAYER_1,
                            System.nanoTime() + Duration.ofMinutes(1).toNanos(), GamePhase.IN_PROGRESS);
                    Disposable current = scheduledTask();
                    assertEquals(Sinks.EmitResult.OK, failSend
                            ? slow.completion.tryEmitError(new IllegalStateException("controlled send failure"))
                            : slow.completion.tryEmitEmpty());
                    assertSame(current, scheduledTask());
                    assertFalse(current.isDisposed());
                })
                .expectComplete().verify(TIMEOUT);
    }

    @ParameterizedTest(name = "고스톱 대기={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("제출 안내부터 송신이 막혀도 바닥 선택·고스톱 대기 타이머는 등록된다")
    void choiceTimerPrecedesEvenInitialSubmitNotification(boolean goStop) {
        slow.observedStatus = "SUBMIT_CARD";
        if (goStop) {
            acquiredCardRepository.addCards(ROOM_ID, 1,
                    List.of(Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.NOV_1, Card.DEC_1)).block(TIMEOUT);
            installedCardRepository.saveHiddenCard(List.of(Card.MAR_3, Card.APR_1), ROOM_ID).block(TIMEOUT);
        } else {
            installedCardRepository.saveRevealedCard(List.of(Card.JAN_1, Card.JAN_2), ROOM_ID).block(TIMEOUT);
        }
        GamePhase expected = goStop ? GamePhase.AWAITING_GO_STOP_CHOICE : GamePhase.AWAITING_FLOOR_CARD_CHOICE;
        StepVerifier.create(submit(ROOM_ID))
                .then(() -> {
                    assertEquals(1, slow.turnStarted.get());
                    assertEquals(expected, gameStateRepository.findById(ROOM_ID).block(TIMEOUT).getPhase());
                    verify(scheduler).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1),
                            eq(Player.PLAYER_1), anyLong(), eq(expected));
                })
                .thenCancel().verify(TIMEOUT);
        assertEquals(1, slow.turnCancelled.get());
        verify(scheduler).cancelAutoPlay(ROOM_ID);
        verifyNoMoreInteractions(scheduler);
    }

    private void cleanupRoom() {
        roomCleanupService.cleanupRoomData(ROOM_ID).block(TIMEOUT);
        sessionManager.removeRoom(ROOM_ID).block(TIMEOUT);
    }

    @ParameterizedTest(name = "자동플레이={0}")
    @ValueSource(booleans = {false, true})
    void endRestartCompletesBeforeBlockedSubmitNotification(boolean autoplay) {
        slow.observedStatus = "SUBMIT_CARD";
        GameState state = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        gameStateRepository.save(state.toBuilder().round(10).currentTurn(2).leadingPlayer(2).build()).block(TIMEOUT);
        StepVerifier.create(turnFlowService.processNormalSubmit(ROOM_ID, Player.PLAYER_1, 0,
                        autoplay ? GameActionSource.AUTOPLAY : GameActionSource.USER, scheduler))
                .then(() -> {
                    assertEquals(1, slow.turnStarted.get());
                    assertEquals(GamePhase.NONE, gameStateRepository.findById(ROOM_ID).block(TIMEOUT).getPhase());
                    assertTrue(installedCardRepository.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).isEmpty());
                    verify(scheduler, never()).scheduleAutoPlay(anyLong(), anyInt(), anyInt(), any(), anyLong(), any());
                })
                .thenCancel().verify(TIMEOUT);
        assertEquals(1, slow.turnCancelled.get());
        GameState recreated = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        assertEquals(Sinks.EmitResult.OK, slow.completion.tryEmitEmpty());
        assertSame(recreated, gameStateRepository.findById(ROOM_ID).block(TIMEOUT));
    }

    private void assertRoomRemoved() {
        assertNull(gameStateRepository.findById(ROOM_ID).block(TIMEOUT));
        assertTrue(sessionManager.getAllUser(ROOM_ID).isEmpty());
        assertNull(sessionManager.getPlayerContext(slow.session).block(TIMEOUT));
        assertNull(sessionManager.getPlayerContext(opponent.session).block(TIMEOUT));
        assertTrue(installedCardRepository.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).isEmpty());
        assertTrue(installedCardRepository.getPlayerCards(ROOM_ID, Player.PLAYER_2).block(TIMEOUT).isEmpty());
    }

    @ParameterizedTest(name = "송신 실패={0}")
    @ValueSource(booleans = {false, true})
    void lateEndNotificationDoesNotRestartRecreatedRoom(boolean failedSend) {
        slow.observedStatus = "SUBMIT_CARD";
        GameState state = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        gameStateRepository.save(state.toBuilder().round(10).currentTurn(2).leadingPlayer(2).build()).block(TIMEOUT);
        GameState[] recreated = new GameState[1];
        StepVerifier.create(submit(ROOM_ID))
                .then(() -> {
                    assertEquals(1, slow.turnStarted.get());
                    assertEquals(GamePhase.NONE, gameStateRepository.findById(ROOM_ID).block(TIMEOUT).getPhase());
                    recreated[0] = GameState.createEmptyRoom(ROOM_ID).toBuilder().round(7).build();
                    gameStateRepository.save(recreated[0]).block(TIMEOUT);
                    installedCardRepository.savePlayerCards(List.of(Card.DEC_1), ROOM_ID, Player.PLAYER_1).block(TIMEOUT);
                    assertEquals(Sinks.EmitResult.OK, failedSend
                            ? slow.completion.tryEmitError(new IllegalStateException("late END send failure"))
                            : slow.completion.tryEmitEmpty());
                }).expectComplete().verify(TIMEOUT);
        assertSame(recreated[0], gameStateRepository.findById(ROOM_ID).block(TIMEOUT));
        assertEquals(List.of(Card.DEC_1), installedCardRepository.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
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
                GameActionSource.USER, targetScheduler);
    }

    private void assertWaitingForSend() {
        assertEquals(1, slow.turnStarted.get(), "턴 안내 송신 구독까지 도달해야 한다");
        assertEquals(0, slow.turnCompleted.get());
        assertEquals(0, slow.turnCancelled.get());
        assertNextTurnSaved();
        verify(scheduler).cancelAutoPlay(ROOM_ID);
        verifyScheduled(ROOM_ID);
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
        String observedStatus = "ANNOUNCE_TURN_INFORMATION";

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
                    boolean turn = message.getPayloadAsText().contains("\"status\":\"" + observedStatus + "\"");
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
