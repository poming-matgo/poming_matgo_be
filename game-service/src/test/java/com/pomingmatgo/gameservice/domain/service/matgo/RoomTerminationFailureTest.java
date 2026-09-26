package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.repository.*;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.global.lock.RoomLockManager;
import com.pomingmatgo.gameservice.global.session.GameConnectionService;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RoomTerminationFailureTest {
    private static final long ROOM_ID = 17L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final GameStateRepository state = mock(GameStateRepository.class);
    private final InstalledCardRepository installed = mock(InstalledCardRepository.class);
    private final AcquiredCardRepository acquired = mock(AcquiredCardRepository.class);
    private final LeadingPlayerRepository leader = mock(LeadingPlayerRepository.class);
    private final RoomLockManager roomLock = mock(RoomLockManager.class);
    private final GameLockCleaner gameLock = mock(GameLockCleaner.class, CALLS_REAL_METHODS);
    private final SessionManager sessions = spy(new SessionManager());
    private final RoomCleanupService cleanup = new RoomCleanupService(
            state, installed, acquired, leader, roomLock, gameLock,
            mock(ApplicationEventPublisher.class), sessions);
    private final WebSocketSession first = mock(WebSocketSession.class);
    private final WebSocketSession second = mock(WebSocketSession.class);
    private final GameService game = mock(GameService.class);

    @BeforeEach
    void setUp() {
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(installed.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(acquired.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(leader.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(roomLock.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(gameLock.cleanup(ROOM_ID)).thenReturn(Mono.empty());
        when(first.getId()).thenReturn("first");
        when(second.getId()).thenReturn("second");
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 101L, first).block(TIMEOUT);
        sessions.addPlayer(ROOM_ID, Player.PLAYER_2, 102L, second).block(TIMEOUT);
        when(game.findGameState(ROOM_ID)).thenReturn(Mono.just(GameState.createEmptyRoom(ROOM_ID)));
        clearInvocations(sessions);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bothEntryPointsRemoveSessionMappingsOnSuccess(boolean disconnect) {
        StepVerifier.create(terminate(disconnect)).expectComplete().verify(TIMEOUT);
        assertMappingsRemoved();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bothEntryPointsRemoveSessionMappingsAfterDataFailure(boolean disconnect) {
        RuntimeException failure = new IllegalStateException("data cleanup failed");
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(failure));
        verifyTermination(disconnect, failure);
        assertMappingsRemoved();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bothEntryPointsRemoveSessionMappingsAfterSynchronousFailure(boolean disconnect) {
        RuntimeException failure = new IllegalStateException("data cleanup construction failed");
        when(state.cleanup(ROOM_ID)).thenThrow(failure);
        verifyTermination(disconnect, failure);
        assertMappingsRemoved();
    }

    @Test
    void lastDisconnectInActionPhaseRemovesRoomEvenAfterDataFailure() {
        sessions.deletePlayer(ROOM_ID, 2, second);
        when(game.findGameState(ROOM_ID)).thenReturn(Mono.just(
                GameState.createEmptyRoom(ROOM_ID).toBuilder().phase(GamePhase.IN_PROGRESS).build()));
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(new IllegalStateException("cleanup failed")));
        MessageSender sender = mock(MessageSender.class);
        when(sender.sendMessageToAllUser(eq(ROOM_ID), any())).thenReturn(Mono.empty());
        GameConnectionService connection = new GameConnectionService(
                game, sessions, sender, cleanup, mock(ReconnectService.class));
        StepVerifier.create(connection.disconnect(first)).expectComplete().verify(TIMEOUT);
        assertMappingsRemoved();
    }

    @Test
    void sessionCleanupStartsOnlyAfterAllDataCleanupTerminates() {
        Sinks.Empty<Void> pending = Sinks.empty();
        RuntimeException failure = new IllegalStateException("data cleanup failed");
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(failure));
        when(installed.cleanup(ROOM_ID)).thenReturn(pending.asMono());
        Mono<Void> result = cleanup.cleanupRoom(ROOM_ID);
        verify(sessions, never()).removeRoom(ROOM_ID);
        verify(state, never()).cleanup(ROOM_ID);
        StepVerifier.create(result)
                .then(() -> {
                    assertEquals(1, pending.currentSubscriberCount());
                    verify(sessions, never()).removeRoom(ROOM_ID);
                    assertSame(second, sessions.getSession(ROOM_ID, 2));
                    assertEquals(Sinks.EmitResult.OK, pending.tryEmitEmpty());
                })
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        assertMappingsRemoved();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void terminationNotificationFailureStillCleansRoom(boolean synchronous) {
        MessageSender sender = mock(MessageSender.class);
        RuntimeException failure = new IllegalStateException("termination notification failed");
        if (synchronous) {
            when(sender.sendMessageToAllUser(eq(ROOM_ID), any())).thenThrow(failure);
        } else {
            when(sender.sendMessageToAllUser(eq(ROOM_ID), any())).thenReturn(Mono.error(failure));
        }
        StepVerifier.create(disconnectInPhase(sender, GamePhase.DETERMINING_STARTING_PLAYER))
                .expectComplete().verify(TIMEOUT);
        verify(state).cleanup(ROOM_ID);
        assertMappingsRemoved();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cleanupWaitsForNotificationTermination(boolean failure) {
        Sinks.Empty<Void> notification = Sinks.empty();
        MessageSender sender = mock(MessageSender.class);
        when(sender.sendMessageToAllUser(eq(ROOM_ID), any())).thenReturn(notification.asMono());
        Mono<Void> result = disconnectInPhase(sender, GamePhase.DETERMINING_STARTING_PLAYER);
        verify(sender, never()).sendMessageToAllUser(eq(ROOM_ID), any());
        StepVerifier.create(result)
                .then(() -> {
                    assertEquals(1, notification.currentSubscriberCount());
                    verify(state, never()).cleanup(ROOM_ID);
                    verify(sessions, never()).removeRoom(ROOM_ID);
                    assertSame(second, sessions.getSession(ROOM_ID, 2));
                    assertEquals(Sinks.EmitResult.OK, failure
                            ? notification.tryEmitError(new IllegalStateException("send failed"))
                            : notification.tryEmitEmpty());
                })
                .expectComplete().verify(TIMEOUT);
        assertMappingsRemoved();
    }

    @Test
    void notificationFailureWaitsForFailingCleanupAndRemovesMappings() {
        Sinks.Empty<Void> pending = Sinks.empty();
        when(installed.cleanup(ROOM_ID)).thenReturn(pending.asMono());
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(new IllegalStateException("cleanup failed")));
        MessageSender sender = mock(MessageSender.class);
        when(sender.sendMessageToAllUser(eq(ROOM_ID), any()))
                .thenReturn(Mono.error(new IllegalStateException("send failed")));
        // 행동 대기 중에도 마지막 접속자 이탈은 종료 경로를 사용한다.
        sessions.deletePlayer(ROOM_ID, 2, second);
        StepVerifier.create(disconnectInPhase(sender, GamePhase.IN_PROGRESS))
                .then(() -> {
                    assertEquals(1, pending.currentSubscriberCount());
                    verify(sessions, never()).removeRoom(ROOM_ID);
                    assertEquals(Sinks.EmitResult.OK, pending.tryEmitEmpty());
                })
                .expectComplete().verify(TIMEOUT);
        assertMappingsRemoved();
    }

    @Test
    void notificationFailurePreservesActionPhaseWithConnectedOpponent() {
        MessageSender sender = mock(MessageSender.class);
        when(sender.sendMessageToAllUser(eq(ROOM_ID), any()))
                .thenReturn(Mono.error(new IllegalStateException("send failed")));
        StepVerifier.create(disconnectInPhase(sender, GamePhase.IN_PROGRESS))
                .expectComplete().verify(TIMEOUT);
        verify(state, never()).cleanup(ROOM_ID);
        verify(sessions, never()).removeRoom(ROOM_ID);
        assertNull(sessions.getSession(ROOM_ID, 1));
        assertSame(second, sessions.getSession(ROOM_ID, 2));
    }

    private Mono<Void> disconnectInPhase(MessageSender sender, GamePhase phase) {
        when(game.findGameState(ROOM_ID)).thenReturn(Mono.just(
                GameState.createEmptyRoom(ROOM_ID).toBuilder().phase(phase).build()));
        return new GameConnectionService(game, sessions, sender, cleanup,
                mock(ReconnectService.class)).disconnect(first);
    }

    @Test
    void dataAndSynchronousSessionFailuresAreBothPreserved() {
        RuntimeException dataFailure = new IllegalStateException("data cleanup failed");
        RuntimeException sessionFailure = new IllegalStateException("session cleanup failed");
        when(state.cleanup(ROOM_ID)).thenReturn(Mono.error(dataFailure));
        doThrow(sessionFailure).when(sessions).removeRoom(ROOM_ID);
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .expectErrorSatisfies(error -> assertEquals(List.of(dataFailure, sessionFailure),
                        Exceptions.unwrapMultipleExcludingTracebacks(error)))
                .verify(TIMEOUT);
        verify(sessions).removeRoom(ROOM_ID);
    }

    @Test
    void sessionFailureIsReturnedAfterSuccessfulDataCleanup() {
        RuntimeException failure = new IllegalStateException("session cleanup failed");
        doReturn(Mono.error(failure)).when(sessions).removeRoom(ROOM_ID);
        StepVerifier.create(cleanup.cleanupRoom(ROOM_ID))
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        verify(state).cleanup(ROOM_ID);
    }

    @Test
    void dataOnlyCleanupPreservesSessionsForGameOverNotification() {
        StepVerifier.create(cleanup.cleanupRoomData(ROOM_ID)).expectComplete().verify(TIMEOUT);
        verify(sessions, never()).removeRoom(ROOM_ID);
        assertSame(first, sessions.getSession(ROOM_ID, 1));
        assertSame(second, sessions.getSession(ROOM_ID, 2));
    }

    private Mono<Void> terminate(boolean disconnect) {
        if (disconnect) {
            return new GameConnectionService(game, sessions, mock(MessageSender.class), cleanup,
                    mock(ReconnectService.class)).disconnect(first);
        }
        return new RoomService(state, sessions, roomLock, cleanup).deleteRoom(ROOM_ID);
    }

    private void verifyTermination(boolean disconnect, RuntimeException failure) {
        // disconnect는 기존 정책대로 오류를 로깅한 뒤 완료하고, HTTP 삭제는 오류를 호출자에게 전달한다.
        if (disconnect) {
            StepVerifier.create(terminate(true)).expectComplete().verify(TIMEOUT);
        } else {
            StepVerifier.create(terminate(false))
                    .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        }
    }

    private void assertMappingsRemoved() {
        verify(sessions).removeRoom(ROOM_ID);
        assertTrue(sessions.getAllUser(ROOM_ID).isEmpty());
        assertNull(sessions.getSession(ROOM_ID, 1));
        assertNull(sessions.getSession(ROOM_ID, 2));
        assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(sessions, "roomSessions")).isEmpty());
        assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(sessions, "sessionToRoomMap")).isEmpty());
        StepVerifier.create(sessions.getPlayerContext(first)).expectComplete().verify(TIMEOUT);
        StepVerifier.create(sessions.getPlayerContext(second)).expectComplete().verify(TIMEOUT);
    }
}
