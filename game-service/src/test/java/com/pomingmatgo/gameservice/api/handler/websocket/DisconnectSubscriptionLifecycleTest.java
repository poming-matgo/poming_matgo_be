package com.pomingmatgo.gameservice.api.handler.websocket;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.pomingmatgo.gameservice.api.handler.websocket.GameWebSocketHandler;
import com.pomingmatgo.gameservice.application.connection.GameConnectionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DisconnectSubscriptionLifecycleTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final GameConnectionService connections = mock(GameConnectionService.class);
    private final WebSocketSession session = mock(WebSocketSession.class);
    private final GameWebSocketHandler handler = new GameWebSocketHandler(
            null, null, null, null, null, null, null, null, connections);
    private final Logger logger = (Logger) LoggerFactory.getLogger(GameWebSocketHandler.class);
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();

    @BeforeEach
    void setUp() {
        when(session.getId()).thenReturn("disconnect-session");
        when(session.receive()).thenReturn(Flux.empty());
        when(connections.disconnect(session)).thenReturn(Mono.empty());
        logs.start();
        logger.addAppender(logs);
    }

    @AfterEach
    void tearDown() {
        handler.shutdown();
        logger.detachAppender(logs);
        logs.stop();
    }

    @Test
    void unsubscribedHandlerDoesNotStartDisconnect() {
        handler.handle(session);
        verifyNoInteractions(connections);
        assertEquals(0, pending());
    }

    @ParameterizedTest
    @ValueSource(strings = {"complete", "error", "cancel"})
    void receiveTerminationStartsIndependentTrackedDisconnect(String termination) {
        Sinks.Empty<Void> disconnect = Sinks.empty();
        AtomicInteger completed = new AtomicInteger();
        when(connections.disconnect(session)).thenReturn(disconnect.asMono()
                .doOnSuccess(ignored -> completed.incrementAndGet()));
        var failure = new IllegalStateException("receive failure");
        Flux<WebSocketMessage> receive = switch (termination) {
            case "error" -> Flux.error(failure);
            case "cancel" -> Flux.never();
            default -> Flux.empty();
        };
        when(session.receive()).thenReturn(receive);

        var verifier = StepVerifier.create(handler.handle(session));
        switch (termination) {
            case "error" -> verifier.expectErrorMatches(error -> error == failure).verify(TIMEOUT);
            case "cancel" -> verifier.thenCancel().verify(TIMEOUT);
            default -> verifier.expectComplete().verify(TIMEOUT);
        }

        await().atMost(TIMEOUT).until(() -> disconnect.currentSubscriberCount() == 1);
        assertEquals(1, pending());
        assertEquals(0, completed.get());
        assertEquals(Sinks.EmitResult.OK, disconnect.tryEmitEmpty());
        await().atMost(TIMEOUT).until(() -> pending() == 0);
        assertEquals(1, completed.get());
        verify(connections).disconnect(session);
        assertTrue(logs.list.isEmpty());
    }

    @Test
    void immediateCompletionDoesNotLeaveTrackedSubscriber() {
        StepVerifier.create(handler.handle(session)).expectComplete().verify(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> {
            verify(connections).disconnect(session);
            assertEquals(0, pending());
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void disconnectFailureIsObservedAndReleased(boolean synchronous) {
        var failure = new IllegalStateException("controlled disconnect failure");
        Sinks.Empty<Void> disconnect = Sinks.empty();
        if (synchronous) {
            when(connections.disconnect(session)).thenThrow(failure);
        } else {
            when(connections.disconnect(session)).thenReturn(disconnect.asMono());
        }

        StepVerifier.create(handler.handle(session)).expectComplete().verify(TIMEOUT);
        if (!synchronous) {
            await().atMost(TIMEOUT).until(() -> disconnect.currentSubscriberCount() == 1);
            assertEquals(1, pending());
            assertEquals(Sinks.EmitResult.OK, disconnect.tryEmitError(failure));
        }
        await().atMost(TIMEOUT).until(() -> pending() == 0);
        assertEquals(1, logs.list.size());
        var event = logs.list.getFirst();
        assertTrue(event.getFormattedMessage().contains("disconnect-session"));
        assertEquals(failure.getMessage(), event.getThrowableProxy().getMessage());
    }

    @Test
    void shutdownCancelsPendingWorkAndRejectsNewDisconnects() {
        Sinks.Empty<Void> first = Sinks.empty();
        Sinks.Empty<Void> second = Sinks.empty();
        AtomicInteger cancelled = new AtomicInteger();
        WebSocketSession other = mock(WebSocketSession.class);
        when(other.receive()).thenReturn(Flux.empty());
        when(connections.disconnect(session)).thenReturn(first.asMono().doOnCancel(cancelled::incrementAndGet));
        when(connections.disconnect(other)).thenReturn(second.asMono().doOnCancel(cancelled::incrementAndGet));
        StepVerifier.create(handler.handle(session)).expectComplete().verify(TIMEOUT);
        StepVerifier.create(handler.handle(other)).expectComplete().verify(TIMEOUT);
        await().atMost(TIMEOUT).until(() -> first.currentSubscriberCount() == 1
                && second.currentSubscriberCount() == 1);
        assertEquals(2, pending());

        handler.shutdown();
        handler.shutdown();

        assertEquals(0, pending());
        assertEquals(2, cancelled.get());
        assertEquals(0, first.currentSubscriberCount());
        assertEquals(0, second.currentSubscriberCount());
        StepVerifier.create(handler.handle(session)).expectComplete().verify(TIMEOUT);
        verify(connections).disconnect(session);
        verify(connections).disconnect(other);
    }

    @Test
    void shutdownDuringPublisherCreationCancelsLateSubscription() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger cancelled = new AtomicInteger();
        Sinks.Empty<Void> disconnect = Sinks.empty();
        when(connections.disconnect(session)).thenAnswer(invocation -> {
            entered.countDown();
            try {
                assertTrue(release.await(3, TimeUnit.SECONDS));
            } catch (InterruptedException ignored) {
                // boundedElastic 작업 취소가 생성 함수를 깨워도 늦게 반환된 Publisher를 검사한다.
                Thread.currentThread().interrupt();
            }
            return disconnect.asMono().doOnCancel(cancelled::incrementAndGet);
        });
        StepVerifier.create(handler.handle(session)).expectComplete().verify(TIMEOUT);
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            assertEquals(1, pending());
            handler.shutdown();
            assertEquals(0, pending());
        } finally {
            release.countDown();
        }
        await().atMost(TIMEOUT).until(() -> cancelled.get() == 1);
        assertEquals(0, disconnect.currentSubscriberCount());
        assertEquals(0, pending());
    }

    private int pending() {
        return ((Disposable.Composite) ReflectionTestUtils.getField(handler, "pendingDisconnects")).size();
    }
}
