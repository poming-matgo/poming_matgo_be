package com.pomingmatgo.gameservice.application.room;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.handler.websocket.WsRoomHandler;
import com.pomingmatgo.gameservice.application.game.InMemoryGameActionExecutor;
import com.pomingmatgo.gameservice.application.pregame.PreGameService;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import com.pomingmatgo.gameservice.infrastructure.messaging.MessageSender;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.*;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryGameLockAspect;
import com.pomingmatgo.gameservice.domain.event.GameActionFailedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("Ready·Join·Leave 실행 소유권 및 정리 경합")
class RoomAdmissionLifecycleBaselineTest {
    private static final long ROOM_ID = 940_048L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final InMemoryRoomExecutionGate gate = new InMemoryRoomExecutionGate();
    private final InMemoryGameActionExecutor executor = new InMemoryGameActionExecutor(gate, this::onFailure);
    private final InMemoryGameStateRepository states = spy(new InMemoryGameStateRepository(new RoomTimerLifecycle(), gate));
    private final InMemoryInstalledCardRepository cards = new InMemoryInstalledCardRepository();
    private final InMemoryLeadingPlayerRepository leaders = spy(new InMemoryLeadingPlayerRepository());
    private final SessionManager sessions = new SessionManager();
    private final RoomCleanupService cleanup = new RoomCleanupService(states, cards,
            new InMemoryAcquiredCardRepository(), leaders, executor, event -> {}, sessions);
    private RoomService rooms;
    private final PreGameService preGame = new PreGameService(leaders, cards, states);
    private final MessageSender sender = mock(MessageSender.class, invocation -> Mono.empty());
    private WsRoomHandler handler;
    private RoomReadyService readyService;

    private void onFailure(Object event) {
        cleanup.onGameActionFailed((GameActionFailedEvent) event);
    }

    @BeforeEach
    void setUp() {
        AspectJProxyFactory roomProxy = new AspectJProxyFactory(new RoomService(states, sessions, cleanup));
        roomProxy.addAspect(new InMemoryGameLockAspect(executor));
        rooms = roomProxy.getProxy();
        AspectJProxyFactory proxy = new AspectJProxyFactory(new RoomReadyService(rooms, preGame));
        proxy.addAspect(new InMemoryGameLockAspect(executor));
        readyService = proxy.getProxy();
        handler = new WsRoomHandler(sender, readyService);
    }
    private final Sinks.Empty<Void> release = Sinks.empty();

    @AfterEach
    void tearDown() {
        release.tryEmitEmpty();
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
        cleanup.shutdown();
        executor.shutdown();
        sessions.shutdown();
    }

    @Test
    @DisplayName("정상 대조군: 두 번째 Ready는 시작 phase와 선택 카드 다섯 장을 저장한다")
    void normalReadyStartsSelection() {
        GameState initial = createReadyRoom();
        ready(initial).block(TIMEOUT);

        assertTrue(current().allPlayersReady());
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, current().getPhase());
        assertEquals(5, leaders.getAllCards(ROOM_ID).block(TIMEOUT).size());
        verify(sender, times(2)).sendMessageToAllUser(eq(ROOM_ID), any());
    }

    @Test
    @DisplayName("Ready 안내 취소 전에 시작 phase와 선택 카드 저장을 완료한다")
    void readySendCancellationPreservesPreparedStart() {
        GameState initial = createReadyRoom();
        pauseSend();
        StepVerifier.create(ready(initial)).then(this::awaitPaused).thenCancel().verify(TIMEOUT);

        assertEquals(0, release.currentSubscriberCount());
        assertTrue(current().allPlayersReady());
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, current().getPhase());
        assertEquals(5, leaders.getAllCards(ROOM_ID).block(TIMEOUT).size());
    }

    @ParameterizedTest(name = "같은 ID 재생성={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("정리는 Ready 안내를 기다리지 않고 늦은 안내는 새 방을 변경하지 않는다")
    void lateReadyNotificationPreservesReplacement(boolean recreate) {
        GameState initial = createReadyRoom();
        pauseSend();
        var verification = StepVerifier.create(ready(initial))
                .then(this::awaitPaused)
                .then(() -> cleanupAndOptionallyRecreate(recreate))
                .then(() -> assertEquals(Sinks.EmitResult.OK, release.tryEmitEmpty()));
        verification.expectComplete().verify(TIMEOUT);

        if (recreate) {
            assertEquals(GamePhase.NONE, current().getPhase());
            assertFalse(current().hasUser(101L));
            assertTrue(current().hasUser(303L));
        } else assertNull(current());
        assertEquals(List.of(), leaders.getAllCards(ROOM_ID).block(TIMEOUT));
    }

    @ParameterizedTest(name = "저장 경계={0}")
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("Ready 수락 후 각 저장 중 취소해도 필수 시작 처리를 완료한다")
    void acceptedReadySurvivesCancellation(int boundary) {
        GameState initial = createReadyRoom();
        pauseReadySave(boundary);
        StepVerifier.create(ready(initial)).then(this::awaitPaused).thenCancel().verify(TIMEOUT);
        assertEquals(1, release.currentSubscriberCount());
        release.tryEmitEmpty();
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, current().getPhase());
            assertEquals(5, leaders.getAllCards(ROOM_ID).block(TIMEOUT).size());
        });
        verifyNoInteractions(sender);
    }

    @ParameterizedTest(name = "저장 경계={0}")
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("정리는 Ready 저장을 기다리며 정리 뒤 고아 카드나 새 방 덮어쓰기가 없다")
    void cleanupWaitsForReadySave(int boundary) {
        GameState initial = createReadyRoom();
        pauseReadySave(boundary);
        StepVerifier.create(ready(initial)).then(this::awaitPaused)
                .then(() -> StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                        .then(() -> {
                            assertNotNull(current());
                            StepVerifier.create(states.create(GameState.createEmptyRoom(ROOM_ID)))
                                    .expectError().verify(TIMEOUT);
                            release.tryEmitEmpty();
                        }).verifyComplete())
                .verifyComplete();
        assertNull(current());
        assertEquals(List.of(), leaders.getAllCards(ROOM_ID).block(TIMEOUT));
        states.create(GameState.createEmptyRoom(ROOM_ID).join(303L)).block(TIMEOUT);
        assertEquals(GamePhase.NONE, current().getPhase());
        assertTrue(current().hasUser(303L));
    }

    @Test
    @DisplayName("최신 조회 중 수락 전 취소는 저장하지 않고 게임·방 락을 해제한다")
    void cancellationBeforeAcceptanceDoesNotSave() {
        GameState initial = createReadyRoom();
        doReturn(release.asMono().thenReturn(initial)).when(states).findById(ROOM_ID);
        StepVerifier.create(ready(initial)).then(this::awaitPaused).thenCancel().verify(TIMEOUT);
        assertEquals(0, release.currentSubscriberCount());
        doCallRealMethod().when(states).findById(ROOM_ID);
        assertFalse(current().allPlayersReady());
        verify(states, never()).save(any());
        ready(initial).block(TIMEOUT);
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, current().getPhase());
    }

    @ParameterizedTest(name = "저장 경계={0}")
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("수락 후 Ready 저장 오류는 실제 자동 정리로 연결된다")
    void acceptedSaveFailureCleansRoom(int boundary) {
        GameState initial = createReadyRoom();
        pauseReadySave(boundary);
        StepVerifier.create(ready(initial)).then(this::awaitPaused)
                .then(() -> release.tryEmitError(new IllegalStateException("save failed")))
                .expectError(IllegalStateException.class).verify(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> assertNull(current()));
        assertEquals(List.of(), leaders.getAllCards(ROOM_ID).block(TIMEOUT));
        verifyNoInteractions(sender);
        states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
    }

    @Test
    @DisplayName("UNREADY도 필수 저장 뒤 안내하며 잘못된 phase는 방을 정리하지 않는다")
    void unreadyAndInvalidPhase() {
        GameState initial = createReadyRoom();
        readyService.readyAndPrepare(ROOM_ID, Player.PLAYER_1, false).block(TIMEOUT);
        assertFalse(current().allPlayersReady());
        readyService.readyAndPrepare(ROOM_ID, Player.PLAYER_1, true).block(TIMEOUT);
        ready(initial).block(TIMEOUT);
        StepVerifier.create(ready(initial)).expectError(
                com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException.class).verify(TIMEOUT);
        assertNotNull(current());
        assertEquals(5, leaders.getAllCards(ROOM_ID).block(TIMEOUT).size());
    }

    private void pauseReadySave(int boundary) {
        if (boundary == 2) {
            doAnswer(invocation -> release.asMono().then(Mono.defer(() -> {
                try { return (Mono<Void>) invocation.callRealMethod(); }
                catch (Throwable error) { return Mono.error(error); }
            }))).when(leaders).saveSelectedCard(anyList(), eq(ROOM_ID));
        } else {
            doAnswer(invocation -> {
                GameState state = invocation.getArgument(0);
                Mono<Long> save = (Mono<Long>) invocation.callRealMethod();
                boolean pause = boundary == 0 ? state.getPhase() == GamePhase.NONE
                        : state.getPhase() == GamePhase.DETERMINING_STARTING_PLAYER;
                return pause ? release.asMono().then(save) : save;
            }).when(states).save(any());
        }
    }

    @Test
    @DisplayName("경쟁 Ready는 저장 전 TRY_AGAIN이며 첫 실행 해제 뒤 재시도할 수 있다")
    void competingReadyCanRetryAfterRelease() {
        GameState initial = GameState.createEmptyRoom(ROOM_ID).join(101L).join(202L);
        states.create(initial).block(TIMEOUT);
        pauseReadySave(0);
        StepVerifier.create(readyService.readyAndPrepare(ROOM_ID, Player.PLAYER_1, true))
                .then(this::awaitPaused)
                .then(() -> StepVerifier.create(ready(initial))
                        .expectErrorSatisfies(error -> assertEquals(
                                com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN,
                                ((com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException) error)
                                        .getWebsocketErrorCode()))
                        .verify(TIMEOUT))
                .then(() -> verify(states, times(1)).save(any()))
                .then(() -> release.tryEmitEmpty())
                .expectNext(false).verifyComplete();
        assertEquals(GamePhase.NONE, current().getPhase());
        assertEquals(List.of(), leaders.getAllCards(ROOM_ID).block(TIMEOUT));
        ready(initial).block(TIMEOUT);
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, current().getPhase());
        assertEquals(5, leaders.getAllCards(ROOM_ID).block(TIMEOUT).size());
    }

    @Test
    @DisplayName("정상 대조군: Join은 빈 자리에 참여자를 저장한다")
    void normalJoinSavesParticipant() {
        createJoinRoom();
        rooms.joinRoom(202L, ROOM_ID).block(TIMEOUT);
        assertTrue(current().hasUser(101L));
        assertTrue(current().hasUser(202L));
        assertEquals(GamePhase.NONE, current().getPhase());
    }

    @Test
    @DisplayName("Join 조회 중 수락 전 취소는 저장하지 않고 락을 해제한다")
    void cancellationBeforeJoinAcceptanceLeavesNoParticipant() {
        createJoinRoom();
        GameState initial = current();
        doReturn(release.asMono().thenReturn(initial)).when(states).findById(ROOM_ID);
        StepVerifier.create(rooms.joinRoom(202L, ROOM_ID))
                .then(this::awaitPaused).thenCancel().verify(TIMEOUT);
        assertEquals(0, release.currentSubscriberCount());
        doCallRealMethod().when(states).findById(ROOM_ID);
        verify(states, never()).save(any());
        rooms.joinRoom(303L, ROOM_ID).block(TIMEOUT);
        assertTrue(current().hasUser(303L));
        assertFalse(current().hasUser(202L));
    }

    @Test
    @DisplayName("Join 수락 후 저장 중 취소해도 참여자 저장을 완료한다")
    void acceptedJoinSurvivesCancellation() {
        createJoinRoom();
        pauseJoinSave();
        StepVerifier.create(rooms.joinRoom(202L, ROOM_ID))
                .then(this::awaitPaused).thenCancel().verify(TIMEOUT);
        assertEquals(1, release.currentSubscriberCount());
        assertFalse(current().hasUser(202L));
        release.tryEmitEmpty();
        await().atMost(TIMEOUT).untilAsserted(() -> assertTrue(current().hasUser(202L)));
        StepVerifier.create(rooms.joinRoom(303L, ROOM_ID))
                .expectErrorSatisfies(error -> assertEquals(ErrorCode.FULL_ROOM,
                        ((BusinessException) error).getErrorCode())).verify(TIMEOUT);
    }

    @ParameterizedTest(name = "같은 ID 재생성={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("정리는 수락한 Join 저장을 기다리고 이후 재생성된 방은 보존한다")
    void cleanupWaitsForJoinSave(boolean recreate) {
        createJoinRoom();
        pauseJoinSave();
        StepVerifier.create(rooms.joinRoom(202L, ROOM_ID))
                .then(this::awaitPaused)
                .then(() -> StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                        .then(() -> {
                            assertNotNull(current());
                            StepVerifier.create(states.create(GameState.createEmptyRoom(ROOM_ID)))
                                    .expectError().verify(TIMEOUT);
                            release.tryEmitEmpty();
                        }).verifyComplete())
                .verifyComplete();

        assertNull(current());
        if (recreate) {
            states.create(GameState.createEmptyRoom(ROOM_ID).join(303L)).block(TIMEOUT);
            assertFalse(current().hasUser(101L));
            assertFalse(current().hasUser(202L));
            assertTrue(current().hasUser(303L));
        }
    }

    @Test
    @DisplayName("Join 수락 후 저장 오류는 방 자동 정리로 연결된다")
    void acceptedJoinFailureCleansRoom() {
        createJoinRoom();
        pauseJoinSave();
        StepVerifier.create(rooms.joinRoom(202L, ROOM_ID)).then(this::awaitPaused)
                .then(() -> release.tryEmitError(new IllegalStateException("join save failed")))
                .expectError(IllegalStateException.class).verify(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> assertNull(current()));
        states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
    }

    @Test
    @DisplayName("Join 검증 실패는 저장하거나 기존 방을 정리하지 않는다")
    void invalidJoinPreservesRoom() {
        StepVerifier.create(rooms.joinRoom(202L, ROOM_ID))
                .expectErrorSatisfies(error -> assertEquals(ErrorCode.NOT_EXISTED_ROOM,
                        ((BusinessException) error).getErrorCode())).verify(TIMEOUT);
        createJoinRoom();
        StepVerifier.create(rooms.joinRoom(101L, ROOM_ID))
                .expectErrorSatisfies(error -> assertEquals(ErrorCode.ALREADY_IN_ROOM,
                        ((BusinessException) error).getErrorCode())).verify(TIMEOUT);
        assertTrue(current().hasUser(101L));
        verify(states, never()).save(any());
    }

    @Test
    @DisplayName("Join 저장 중 같은 방 Join·Ready는 거부하고 다른 방은 독립 실행한다")
    void joinSerializesWithReadyAndOtherJoins() {
        createJoinRoom();
        pauseJoinSave();
        long otherRoom = ROOM_ID + 1;
        states.create(GameState.createEmptyRoom(otherRoom)).block(TIMEOUT);
        try {
            StepVerifier.create(rooms.joinRoom(202L, ROOM_ID)).then(this::awaitPaused)
                    .then(() -> {
                        StepVerifier.create(rooms.joinRoom(303L, ROOM_ID))
                                .expectErrorSatisfies(this::assertTryAgain).verify(TIMEOUT);
                        StepVerifier.create(readyService.readyAndPrepare(ROOM_ID, Player.PLAYER_1, true))
                                .expectErrorSatisfies(this::assertTryAgain).verify(TIMEOUT);
                        StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID))
                                .expectErrorSatisfies(this::assertTryAgain).verify(TIMEOUT);
                        doCallRealMethod().when(states).save(argThat(state -> state.getRoomId() == otherRoom));
                        rooms.joinRoom(404L, otherRoom).block(TIMEOUT);
                        assertTrue(states.findById(otherRoom).block(TIMEOUT).hasUser(404L));
                    })
                    .then(() -> release.tryEmitEmpty()).verifyComplete();
            readyService.readyAndPrepare(ROOM_ID, Player.PLAYER_1, true).block(TIMEOUT);
            assertTrue(current().getPlayerState(Player.PLAYER_1).isReady());
        } finally {
            cleanup.cleanupRoom(otherRoom).block(TIMEOUT);
        }
    }

    private void assertTryAgain(Throwable error) {
        assertEquals(WebSocketErrorCode.TRY_AGAIN,
                ((WebSocketBusinessException) error).getWebsocketErrorCode());
    }

    @Test
    @DisplayName("Ready 저장 중 Leave는 거부되고 시작 완료 뒤에는 진행 중 오류를 반환한다")
    void leaveCannotOverwriteReady() {
        GameState initial = createReadyRoom();
        pauseReadySave(0);
        StepVerifier.create(ready(initial)).then(this::awaitPaused)
                .then(() -> StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID))
                        .expectErrorSatisfies(this::assertTryAgain).verify(TIMEOUT))
                .then(() -> release.tryEmitEmpty()).verifyComplete();
        StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID))
                .expectErrorSatisfies(error -> assertEquals(ErrorCode.GAME_IN_PROGRESS,
                        ((BusinessException) error).getErrorCode())).verify(TIMEOUT);
        assertTrue(current().hasUser(101L));
        assertTrue(current().allPlayersReady());
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, current().getPhase());
    }

    @Test
    @DisplayName("Leave 수락 후 저장 중 취소해도 퇴장을 완료하고 방은 유지한다")
    void acceptedLeaveSurvivesCancellation() {
        createJoinRoom();
        pauseJoinSave();
        StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID))
                .then(this::awaitPaused).thenCancel().verify(TIMEOUT);
        assertEquals(1, release.currentSubscriberCount());
        assertTrue(current().hasUser(101L));
        release.tryEmitEmpty();
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertNotNull(current());
            assertFalse(current().hasUser(101L));
        });
        rooms.joinRoom(303L, ROOM_ID).block(TIMEOUT);
        assertTrue(current().hasUser(303L));
    }

    @Test
    @DisplayName("Leave 저장 중 Join·Ready·Leave는 TRY_AGAIN이고 다른 방은 독립 실행한다")
    void leaveSerializesWithAdmission() {
        createJoinRoom();
        pauseJoinSave();
        long otherRoom = ROOM_ID + 1;
        states.create(GameState.createEmptyRoom(otherRoom).join(404L)).block(TIMEOUT);
        try {
            StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID)).then(this::awaitPaused)
                    .then(() -> {
                        StepVerifier.create(rooms.joinRoom(202L, ROOM_ID))
                                .expectErrorSatisfies(this::assertTryAgain).verify(TIMEOUT);
                        StepVerifier.create(readyService.readyAndPrepare(ROOM_ID, Player.PLAYER_1, true))
                                .expectErrorSatisfies(this::assertTryAgain).verify(TIMEOUT);
                        StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID))
                                .expectErrorSatisfies(this::assertTryAgain).verify(TIMEOUT);
                        doCallRealMethod().when(states).save(argThat(state -> state.getRoomId() == otherRoom));
                        rooms.leaveRoom(404L, otherRoom).block(TIMEOUT);
                        assertFalse(states.findById(otherRoom).block(TIMEOUT).hasUser(404L));
                    }).then(() -> release.tryEmitEmpty()).verifyComplete();
            rooms.joinRoom(202L, ROOM_ID).block(TIMEOUT);
            assertFalse(current().hasUser(101L));
            assertTrue(current().hasUser(202L));
        } finally {
            cleanup.cleanupRoom(otherRoom).block(TIMEOUT);
        }
    }

    @ParameterizedTest(name = "같은 ID 재생성={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("전체 정리는 수락한 Leave 저장을 기다리고 재생성 방을 보존한다")
    void cleanupWaitsForLeaveSave(boolean recreate) {
        createJoinRoom();
        pauseJoinSave();
        StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID)).then(this::awaitPaused)
                .then(() -> StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                        .then(() -> {
                            assertNotNull(current());
                            StepVerifier.create(states.create(GameState.createEmptyRoom(ROOM_ID)))
                                    .expectError().verify(TIMEOUT);
                            release.tryEmitEmpty();
                        }).verifyComplete())
                .verifyComplete();
        assertNull(current());
        if (recreate) {
            states.create(GameState.createEmptyRoom(ROOM_ID).join(303L)).block(TIMEOUT);
            assertTrue(current().hasUser(303L));
            assertFalse(current().hasUser(101L));
        }
    }

    @Test
    @DisplayName("Leave 조회 중 수락 전 취소는 저장하지 않고 락을 해제한다")
    void cancellationBeforeLeaveAcceptanceDoesNotSave() {
        createJoinRoom();
        GameState initial = current();
        doReturn(release.asMono().thenReturn(initial)).when(states).findById(ROOM_ID);
        StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID))
                .then(this::awaitPaused).thenCancel().verify(TIMEOUT);
        assertEquals(0, release.currentSubscriberCount());
        doCallRealMethod().when(states).findById(ROOM_ID);
        verify(states, never()).save(any());
        assertTrue(current().hasUser(101L));
        rooms.leaveRoom(101L, ROOM_ID).block(TIMEOUT);
        assertFalse(current().hasUser(101L));
    }

    @Test
    @DisplayName("Leave 수락 후 저장 오류는 방 자동 정리로 연결된다")
    void acceptedLeaveFailureCleansRoom() {
        createJoinRoom();
        pauseJoinSave();
        StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID)).then(this::awaitPaused)
                .then(() -> release.tryEmitError(new IllegalStateException("leave save failed")))
                .expectError(IllegalStateException.class).verify(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> assertNull(current()));
        states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
    }

    @Test
    @DisplayName("Leave는 구독 시 최신 phase를 검사하고 잘못된 요청은 방을 보존한다")
    void leaveValidatesFreshStateAndPreservesRoom() {
        StepVerifier.create(rooms.leaveRoom(101L, ROOM_ID))
                .expectErrorSatisfies(error -> assertEquals(ErrorCode.NOT_EXISTED_ROOM,
                        ((BusinessException) error).getErrorCode())).verify(TIMEOUT);
        GameState initial = createReadyRoom();
        rooms.leaveRoom(303L, ROOM_ID).block(TIMEOUT);
        verify(states, never()).save(any());
        Mono<Void> delayedLeave = rooms.leaveRoom(101L, ROOM_ID);
        ready(initial).block(TIMEOUT);
        clearInvocations(states);
        StepVerifier.create(delayedLeave)
                .expectErrorSatisfies(error -> assertEquals(ErrorCode.GAME_IN_PROGRESS,
                        ((BusinessException) error).getErrorCode())).verify(TIMEOUT);
        rooms.leaveRoom(303L, ROOM_ID).block(TIMEOUT);
        assertTrue(current().hasUser(101L));
        assertTrue(current().hasUser(202L));
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, current().getPhase());
        verify(states, never()).save(any());
    }

    private GameState createReadyRoom() {
        GameState state = GameState.createEmptyRoom(ROOM_ID).join(101L).join(202L)
                .withPlayerReady(Player.PLAYER_1, true);
        states.create(state).block(TIMEOUT);
        return state;
    }

    private void createJoinRoom() {
        states.create(GameState.createEmptyRoom(ROOM_ID).join(101L)).block(TIMEOUT);
    }

    private Mono<Void> ready(GameState state) {
        RequestEvent<Void> event = new RequestEvent<>();
        event.setSubCategory(SubCategory.READY);
        return handler.handleRoomEvent(event, state, Player.PLAYER_2);
    }

    private void pauseSend() {
        when(sender.sendMessageToAllUser(eq(ROOM_ID), any())).thenReturn(release.asMono());
    }

    private void pauseJoinSave() {
        doAnswer(invocation -> {
            Mono<Long> save = (Mono<Long>) invocation.callRealMethod();
            return release.asMono().then(save);
        }).when(states).save(any());
    }

    private void cleanupAndOptionallyRecreate(boolean recreate) {
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
        assertNull(current());
        assertEquals(1, release.currentSubscriberCount(), "정리가 대기 중 요청을 회수하지 않는다");
        if (recreate) states.create(GameState.createEmptyRoom(ROOM_ID).join(303L)).block(TIMEOUT);
    }

    private void awaitPaused() {
        await().atMost(TIMEOUT).until(() -> release.currentSubscriberCount() == 1);
    }

    private GameState current() {
        return states.findById(ROOM_ID).block(TIMEOUT);
    }
}
