package com.pomingmatgo.gameservice.global.session;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.pomingmatgo.gameservice.domain.Player;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SessionReplacementFailureTest {
    private static final long ROOM_ID = 19L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final SessionManager sessions = new SessionManager();
    private final WebSocketSession old = mock(WebSocketSession.class);
    private final WebSocketSession current = mock(WebSocketSession.class);
    private final Logger logger = (Logger) LoggerFactory.getLogger(SessionManager.class);
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();

    @BeforeEach
    void setUp() {
        logs.start();
        logger.addAppender(logs);
        when(old.getId()).thenReturn("replaced-session");
        when(current.getId()).thenReturn("current-session");
        when(old.close()).thenReturn(Mono.empty());
        complete(sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 101L, old));
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(logs);
        logs.stop();
    }

    @Test
    void replacementAndCloseStartOnlyOnSubscription() {
        Mono<Void> replacement = replace();
        assertSame(old, sessions.getSession(ROOM_ID, 1));
        verify(old, never()).close();

        complete(replacement);

        verify(old).close();
        assertCurrentSession();
        assertTrue(logs.list.isEmpty());
    }

    @Test
    void synchronousCloseFailureIsLoggedWithoutFailingReplacement() {
        var failure = new IllegalStateException("synchronous close failure");
        when(old.close()).thenThrow(failure);

        complete(replace());

        assertCurrentSession();
        assertCloseFailureLogged(failure);
    }

    @Test
    void asynchronousCloseFailureIsLoggedAfterReplacementCompleted() {
        Sinks.Empty<Void> close = Sinks.empty();
        when(old.close()).thenReturn(close.asMono());
        complete(replace());
        assertEquals(1, close.currentSubscriberCount());
        assertCurrentSession();

        var failure = new IllegalStateException("asynchronous close failure");
        assertEquals(Sinks.EmitResult.OK, close.tryEmitError(failure));

        assertEquals(0, close.currentSubscriberCount());
        assertCurrentSession();
        assertCloseFailureLogged(failure);
    }

    @Test
    void delayedCloseAndStaleDeletePreserveReplacement() {
        Sinks.Empty<Void> close = Sinks.empty();
        when(old.close()).thenReturn(close.asMono()
                .doOnSuccess(ignored -> sessions.deletePlayer(ROOM_ID, 1, old)));

        complete(replace());
        assertEquals(1, close.currentSubscriberCount());
        assertCurrentSession();
        assertEquals(Sinks.EmitResult.OK, close.tryEmitEmpty());

        assertEquals(0, close.currentSubscriberCount());
        assertCurrentSession();
        verify(current, never()).close();
    }

    @Test
    void lateCloseFailureDoesNotRecreateRemovedRoomMappings() {
        Sinks.Empty<Void> close = Sinks.empty();
        when(old.close()).thenReturn(close.asMono());
        complete(replace());
        complete(sessions.removeRoom(ROOM_ID));

        var failure = new IllegalStateException("close after room removal");
        assertEquals(Sinks.EmitResult.OK, close.tryEmitError(failure));

        assertNull(sessions.getSession(ROOM_ID, 1));
        StepVerifier.create(sessions.getPlayerContext(old)).expectComplete().verify(TIMEOUT);
        StepVerifier.create(sessions.getPlayerContext(current)).expectComplete().verify(TIMEOUT);
        assertTrue(sessions.getAllUser(ROOM_ID).isEmpty());
        assertCloseFailureLogged(failure);
    }

    @Test
    void registeringSameSessionDoesNotCloseIt() {
        complete(sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 101L, old));

        verify(old, never()).close();
        assertSame(old, sessions.getSession(ROOM_ID, 1));
        assertTrue(logs.list.isEmpty());
    }

    private Mono<Void> replace() {
        return sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 101L, current);
    }

    private void assertCurrentSession() {
        assertSame(current, sessions.getSession(ROOM_ID, 1));
        StepVerifier.create(sessions.getPlayerContext(old)).expectComplete().verify(TIMEOUT);
        StepVerifier.create(sessions.getPlayerContext(current))
                .expectNext(new SessionManager.PlayerContext(ROOM_ID, 101L, 1))
                .expectComplete().verify(TIMEOUT);
    }

    private void assertCloseFailureLogged(Throwable failure) {
        assertEquals(1, logs.list.size());
        ILoggingEvent event = logs.list.getFirst();
        assertTrue(event.getFormattedMessage().contains("replaced-session"));
        assertTrue(event.getFormattedMessage().contains(Long.toString(ROOM_ID)));
        assertNotNull(event.getThrowableProxy());
        assertEquals(failure.getMessage(), event.getThrowableProxy().getMessage());
    }

    private void complete(Mono<Void> operation) {
        StepVerifier.create(operation).expectComplete().verify(TIMEOUT);
    }
}
