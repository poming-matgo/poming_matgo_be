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
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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
        sessions.shutdown();
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
        assertEquals(0, pendingCloses());
    }

    @Test
    void synchronousCloseFailureIsLoggedWithoutFailingReplacement() {
        var failure = new IllegalStateException("synchronous close failure");
        when(old.close()).thenThrow(failure);

        complete(replace());

        assertCurrentSession();
        assertCloseFailureLogged(failure);
        assertEquals(0, pendingCloses());
    }

    @Test
    void asynchronousCloseFailureIsLoggedAfterReplacementCompleted() {
        Sinks.Empty<Void> close = Sinks.empty();
        when(old.close()).thenReturn(close.asMono());
        complete(replace());
        assertEquals(1, close.currentSubscriberCount());
        assertEquals(1, pendingCloses());
        assertCurrentSession();

        var failure = new IllegalStateException("asynchronous close failure");
        assertEquals(Sinks.EmitResult.OK, close.tryEmitError(failure));

        assertEquals(0, close.currentSubscriberCount());
        assertCurrentSession();
        assertCloseFailureLogged(failure);
        assertEquals(0, pendingCloses());
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
        assertEquals(0, pendingCloses());
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

    @Test
    void neverEndingCloseTimesOutAndReleasesTrackingWithoutChangingReplacement() {
        Sinks.Empty<Void> close = Sinks.empty();
        AtomicInteger cancelled = new AtomicInteger();
        when(old.close()).thenReturn(close.asMono().doOnCancel(cancelled::incrementAndGet));

        StepVerifier.withVirtualTime(() -> replace().then(Mono.delay(Duration.ofSeconds(6))))
                .then(() -> {
                    assertCurrentSession();
                    assertEquals(1, pendingCloses());
                    assertEquals(1, close.currentSubscriberCount());
                })
                .thenAwait(Duration.ofSeconds(5))
                .then(() -> {
                    assertEquals(1, cancelled.get());
                    assertEquals(0, close.currentSubscriberCount());
                    assertEquals(0, pendingCloses());
                    assertEquals(1, logs.list.size());
                    assertEquals(TimeoutException.class.getName(),
                            logs.list.getFirst().getThrowableProxy().getClassName());
                    assertTrue(logs.list.getFirst().getFormattedMessage().contains("replaced-session"));
                    assertCurrentSession();
                })
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(0L).expectComplete().verify(TIMEOUT);
        assertEquals(Sinks.EmitResult.OK, close.tryEmitEmpty());
        assertEquals(0, pendingCloses());
        verify(current, never()).close();
    }

    @Test
    void shutdownCancelsAllPendingClosesEvenAfterRoomRemoval() {
        Sinks.Empty<Void> first = Sinks.empty();
        Sinks.Empty<Void> second = Sinks.empty();
        when(old.close()).thenReturn(first.asMono());
        when(current.close()).thenReturn(second.asMono());
        WebSocketSession newest = mock(WebSocketSession.class);
        when(newest.getId()).thenReturn("newest-session");
        complete(replace());
        complete(sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 101L, newest));
        complete(sessions.removeRoom(ROOM_ID));
        assertEquals(2, pendingCloses());

        sessions.shutdown();
        sessions.shutdown();

        assertEquals(0, first.currentSubscriberCount());
        assertEquals(0, second.currentSubscriberCount());
        assertEquals(0, pendingCloses());
        assertTrue(logs.list.isEmpty());
        verify(newest, never()).close();
    }

    @Test
    void shutdownPreventsNewIndependentCloseSubscriptions() {
        sessions.shutdown();
        complete(replace());

        verify(old, never()).close();
        assertEquals(0, pendingCloses());
        assertCurrentSession();
    }

    @Test
    void shutdownDuringClosePublisherCreationCancelsLateSubscription() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Sinks.Empty<Void> close = Sinks.empty();
        when(old.close()).thenAnswer(invocation -> {
            entered.countDown();
            assertTrue(release.await(3, TimeUnit.SECONDS));
            return close.asMono();
        });
        try (var executor = Executors.newSingleThreadExecutor()) {
            var registration = executor.submit(() -> complete(replace()));
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                assertEquals(1, pendingCloses());
                sessions.shutdown();
                assertEquals(0, pendingCloses());
            } finally {
                release.countDown();
            }
            registration.get(3, TimeUnit.SECONDS);
        }
        assertEquals(0, close.currentSubscriberCount());
        assertEquals(0, pendingCloses());
        assertCurrentSession();
    }

    private int pendingCloses() {
        return ((Disposable.Composite) ReflectionTestUtils.getField(sessions, "pendingCloses")).size();
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
