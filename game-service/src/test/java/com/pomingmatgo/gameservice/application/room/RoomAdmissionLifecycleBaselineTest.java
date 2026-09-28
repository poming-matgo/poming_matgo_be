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
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomLockManager;
import com.pomingmatgo.gameservice.infrastructure.messaging.MessageSender;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.*;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
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

// 결함 기대값은 후속 소유권 구현 시 정상 보장으로 전환한다.
@DisplayName("Ready/Join 취소·정리 경합 기준선 (보호 구현 전)")
class RoomAdmissionLifecycleBaselineTest {
    private static final long ROOM_ID = 940_048L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final InMemoryRoomExecutionGate gate = new InMemoryRoomExecutionGate();
    private final InMemoryGameActionExecutor executor = new InMemoryGameActionExecutor(gate, event -> {});
    private final InMemoryGameStateRepository states = spy(new InMemoryGameStateRepository(new RoomTimerLifecycle(), gate));
    private final InMemoryInstalledCardRepository cards = new InMemoryInstalledCardRepository();
    private final InMemoryLeadingPlayerRepository leaders = new InMemoryLeadingPlayerRepository();
    private final InMemoryRoomLockManager lock = new InMemoryRoomLockManager();
    private final SessionManager sessions = new SessionManager();
    private final RoomCleanupService cleanup = new RoomCleanupService(states, cards,
            new InMemoryAcquiredCardRepository(), leaders, lock, executor, event -> {}, sessions);
    private final RoomService rooms = new RoomService(states, sessions, lock, cleanup);
    private final PreGameService preGame = new PreGameService(leaders, cards, states, lock);
    private final MessageSender sender = mock(MessageSender.class, invocation -> Mono.empty());
    private final WsRoomHandler handler = new WsRoomHandler(sender, rooms, preGame, lock);
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
    @DisplayName("기준선: Ready 안내 중 취소하면 두 명 준비 상태만 남고 시작하지 않는다")
    void readySendCancellationLeavesReadyFlagsWithoutStart() {
        GameState initial = createReadyRoom();
        pauseSend();
        StepVerifier.create(ready(initial)).then(this::awaitPaused).thenCancel().verify(TIMEOUT);

        assertEquals(0, release.currentSubscriberCount());
        assertTrue(current().allPlayersReady());
        assertEquals(GamePhase.NONE, current().getPhase());
        assertEquals(List.of(), leaders.getAllCards(ROOM_ID).block(TIMEOUT));
        // 취소 후 같은 방 락을 다시 얻어 상태를 변경할 수 있다.
        lock.withLock(ROOM_ID, rooms.readyFresh(ROOM_ID, Player.PLAYER_2, false),
                IllegalStateException::new).block(TIMEOUT);
        assertFalse(current().allPlayersReady());
    }

    @ParameterizedTest(name = "같은 ID 재생성={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("기준선: 정리는 Ready 안내를 기다리지 않고 재생성 시 늦은 시작이 새 방을 덮어쓴다")
    void cleanupOvertakesReadyAndLateStartCanOverwriteReplacement(boolean recreate) {
        GameState initial = createReadyRoom();
        pauseSend();
        var verification = StepVerifier.create(ready(initial))
                .then(this::awaitPaused)
                .then(() -> cleanupAndOptionallyRecreate(recreate))
                .then(() -> assertEquals(Sinks.EmitResult.OK, release.tryEmitEmpty()));
        if (recreate) verification.expectComplete().verify(TIMEOUT);
        else verification.expectError(BusinessException.class).verify(TIMEOUT);

        if (recreate) {
            assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, current().getPhase());
            assertTrue(current().hasUser(101L), "이전 방 참여자가 새 방에 복원되는 결함");
            assertFalse(current().hasUser(303L));
            assertEquals(5, leaders.getAllCards(ROOM_ID).block(TIMEOUT).size());
        } else {
            assertNull(current(), "save는 삭제된 방을 부활시키지 않는다");
            assertEquals(List.of(), leaders.getAllCards(ROOM_ID).block(TIMEOUT));
        }
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
    @DisplayName("Join 저장 전 취소는 참여자를 남기지 않고 방 락을 해제한다")
    void cancellationBeforeJoinSaveLeavesNoParticipant() {
        createJoinRoom();
        pauseJoinSave();
        StepVerifier.create(rooms.joinRoom(202L, ROOM_ID))
                .then(this::awaitPaused).thenCancel().verify(TIMEOUT);
        assertEquals(0, release.currentSubscriberCount());
        assertFalse(current().hasUser(202L));
        release.tryEmitEmpty();
        rooms.joinRoom(303L, ROOM_ID).block(TIMEOUT);
        assertTrue(current().hasUser(303L));
        assertFalse(current().hasUser(202L));
    }

    @ParameterizedTest(name = "같은 ID 재생성={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("기준선: 정리는 Join 저장을 기다리지 않고 늦은 저장이 재생성된 방을 덮어쓴다")
    void cleanupOvertakesJoinSave(boolean recreate) {
        createJoinRoom();
        pauseJoinSave();
        var verification = StepVerifier.create(rooms.joinRoom(202L, ROOM_ID))
                .then(this::awaitPaused)
                .then(() -> cleanupAndOptionallyRecreate(recreate))
                .then(() -> assertEquals(Sinks.EmitResult.OK, release.tryEmitEmpty()));
        if (recreate) verification.expectComplete().verify(TIMEOUT);
        else verification.expectError(BusinessException.class).verify(TIMEOUT);

        if (recreate) {
            assertTrue(current().hasUser(101L));
            assertTrue(current().hasUser(202L));
            assertFalse(current().hasUser(303L), "새 방 참여자가 이전 Join 저장으로 사라지는 결함");
        } else assertNull(current());
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
