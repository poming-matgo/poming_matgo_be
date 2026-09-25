package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.scheduler.RoomTimerLifecycle;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.repository.*;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.global.lock.RoomLockManager;
import com.pomingmatgo.gameservice.global.session.GameConnectionService;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

// 정리 시작 후 취소는 완료를 보장한다. 선행 종료 안내 중 취소는 아직 결함 재현이다.
class RoomTerminationCancellationTest {
    private static final long ROOM_ID = 18L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final InMemoryGameStateRepository state = new InMemoryGameStateRepository(new RoomTimerLifecycle());
    private final InMemoryInstalledCardRepository installed = spy(new InMemoryInstalledCardRepository());
    private final AcquiredCardRepository acquired = mock(AcquiredCardRepository.class);
    private final LeadingPlayerRepository leader = mock(LeadingPlayerRepository.class);
    private final RoomLockManager roomLock = mock(RoomLockManager.class);
    private final GameLockCleaner gameLock = mock(GameLockCleaner.class);
    private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
    private final SessionManager sessions = spy(new SessionManager());
    private final MessageSender sender = mock(MessageSender.class);
    private final GameService game = mock(GameService.class);
    private final WebSocketSession first = mock(WebSocketSession.class);
    private final WebSocketSession second = mock(WebSocketSession.class);
    private final RoomCleanupService cleanup = new RoomCleanupService(
            state, installed, acquired, leader, roomLock, gameLock, events, sessions);
    private final Sinks.Empty<Void> gate = Sinks.empty();
    private final AtomicInteger cancelled = new AtomicInteger();
    private final AtomicInteger completed = new AtomicInteger();

    @BeforeEach
    void setUp() {
        when(acquired.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(leader.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(roomLock.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(gameLock.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(sender.sendMessageToAllUser(eq(ROOM_ID), any())).thenReturn(Mono.empty());
        when(game.findGameState(ROOM_ID)).thenAnswer(invocation -> state.findById(ROOM_ID));
        when(first.getId()).thenReturn("cancel-first");
        when(second.getId()).thenReturn("cancel-second");
        state.create(GameState.createEmptyRoom(ROOM_ID).toBuilder()
                .phase(GamePhase.DETERMINING_STARTING_PLAYER).build()).block(TIMEOUT);
        installed.savePlayerCards(List.of(Card.JAN_3), ROOM_ID, Player.PLAYER_1).block(TIMEOUT);
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 101L, first).block(TIMEOUT);
        sessions.addPlayer(ROOM_ID, Player.PLAYER_2, 102L, second).block(TIMEOUT);
        clearInvocations(sessions);
    }

    @ParameterizedTest(name = "disconnect={0}")
    @ValueSource(booleans = {false, true})
    void cancellationDuringDataCleanupStillRemovesCardsAndSessionMappings(boolean disconnect) {
        pauseCardCleanup();
        StepVerifier.create(terminate(disconnect))
                .then(() -> {
                    assertEquals(1, gate.currentSubscriberCount());
                    assertPartialCleanup(disconnect);
                })
                .thenCancel().verify(TIMEOUT);

        assertEquals(0, cancelled.get());
        assertEquals(1, gate.currentSubscriberCount());
        assertEquals(0, completed.get());
        assertPartialCleanup(disconnect);
        assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
        assertFullyCleaned();
        assertEquals(0, completed.get(), "취소한 호출자에게 완료 신호를 전달하지 않는다");
    }

    @ParameterizedTest(name = "disconnect={0}")
    @ValueSource(booleans = {false, true})
    void resumedDataCleanupRemovesCardsAndSessionMappings(boolean disconnect) {
        pauseCardCleanup();
        StepVerifier.create(terminate(disconnect))
                .then(() -> {
                    assertEquals(1, gate.currentSubscriberCount());
                    assertPartialCleanup(disconnect);
                    assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
                })
                .expectComplete().verify(TIMEOUT);
        assertEquals(0, cancelled.get());
        assertEquals(1, completed.get());
        assertFullyCleaned();
    }

    @ParameterizedTest(name = "cancel={0}")
    @ValueSource(booleans = {false, true})
    void notificationTerminationControlsWhetherCleanupStarts(boolean cancel) {
        when(sender.sendMessageToAllUser(eq(ROOM_ID), any())).thenReturn(waitAtGate());
        var verifier = StepVerifier.create(terminate(true)).then(() -> {
            assertEquals(1, gate.currentSubscriberCount());
            assertBeforeDataCleanup();
        });
        if (cancel) {
            verifier.thenCancel().verify(TIMEOUT);
            assertEquals(1, cancelled.get());
            assertEquals(0, gate.currentSubscriberCount());
            assertBeforeDataCleanup();
            assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
            assertBeforeDataCleanup();
            assertEquals(0, completed.get());
        } else {
            verifier.then(() -> assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty()))
                    .expectComplete().verify(TIMEOUT);
            assertEquals(0, cancelled.get());
            assertEquals(1, completed.get());
            assertFullyCleaned();
        }
    }

    private void pauseCardCleanup() {
        Mono<Void> actualCleanup = installed.cleanup(ROOM_ID);
        doReturn(waitAtGate().then(actualCleanup)).when(installed).cleanup(ROOM_ID);
        clearInvocations(installed);
    }

    private Mono<Void> waitAtGate() {
        return gate.asMono().doOnCancel(cancelled::incrementAndGet);
    }

    private Mono<Void> terminate(boolean disconnect) {
        Mono<Void> result = disconnect
                ? new GameConnectionService(game, sessions, sender, cleanup,
                        mock(ReconnectService.class)).disconnect(first)
                : new RoomService(state, sessions, roomLock, cleanup).deleteRoom(ROOM_ID);
        return result.doOnSuccess(ignored -> completed.incrementAndGet());
    }

    private void assertPartialCleanup(boolean disconnect) {
        StepVerifier.create(state.findById(ROOM_ID)).expectComplete().verify(TIMEOUT);
        assertEquals(List.of(Card.JAN_3), installed.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        verify(events).publishEvent(new RoomCleanedUpEvent(ROOM_ID));
        verify(sessions, never()).removeRoom(ROOM_ID);
        assertMappingsRetained(disconnect);
    }

    private void assertBeforeDataCleanup() {
        assertNotNull(state.findById(ROOM_ID).block(TIMEOUT));
        assertEquals(List.of(Card.JAN_3), installed.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        verify(installed, never()).cleanup(ROOM_ID);
        verifyNoInteractions(events);
        verify(sessions, never()).removeRoom(ROOM_ID);
        assertMappingsRetained(true);
    }

    private void assertMappingsRetained(boolean disconnect) {
        assertSame(disconnect ? null : first, sessions.getSession(ROOM_ID, 1));
        assertSame(second, sessions.getSession(ROOM_ID, 2));
        assertEquals(disconnect ? null : new SessionManager.PlayerContext(ROOM_ID, 101L, 1),
                sessions.getPlayerContext(first).block(TIMEOUT));
        assertEquals(new SessionManager.PlayerContext(ROOM_ID, 102L, 2),
                sessions.getPlayerContext(second).block(TIMEOUT));
        assertEquals(1, sessionMap("roomSessions").size());
        assertEquals(disconnect ? 1 : 2, sessionMap("sessionToRoomMap").size());
    }

    private void assertFullyCleaned() {
        StepVerifier.create(state.findById(ROOM_ID)).expectComplete().verify(TIMEOUT);
        assertEquals(List.of(), installed.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT));
        verify(events).publishEvent(new RoomCleanedUpEvent(ROOM_ID));
        verify(sessions).removeRoom(ROOM_ID);
        assertTrue(sessionMap("roomSessions").isEmpty());
        assertTrue(sessionMap("sessionToRoomMap").isEmpty());
        StepVerifier.create(sessions.getPlayerContext(first)).expectComplete().verify(TIMEOUT);
        StepVerifier.create(sessions.getPlayerContext(second)).expectComplete().verify(TIMEOUT);
    }

    private Map<?, ?> sessionMap(String field) {
        return (Map<?, ?>) ReflectionTestUtils.getField(sessions, field);
    }
}
