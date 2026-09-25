package com.pomingmatgo.gameservice.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import com.pomingmatgo.gameservice.global.metrics.ThroughputMetricsRouter;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MessageSenderOutcomeTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final ThroughputRecorder recorder = new ThroughputRecorder();
    private final WebSocketSession session = mock(WebSocketSession.class);
    private MessageSender sender;

    @BeforeEach
    void setUp() {
        sender = sender(true);
        when(session.isOpen()).thenReturn(true);
        when(session.textMessage(anyString())).thenAnswer(invocation -> new WebSocketMessage(
                WebSocketMessage.Type.TEXT, DefaultDataBufferFactory.sharedInstance.wrap(
                        invocation.<String>getArgument(0).getBytes(StandardCharsets.UTF_8))));
        when(session.send(any())).thenReturn(Mono.empty());
    }

    @Test
    void countsOnlySubscribedSuccessfulSendsIncludingResubscription() {
        Mono<Void> send = sender.sendPayload(session, "payload");
        assertCounts(0, 0, 0, 0);
        verify(session, never()).send(any());
        send.block(TIMEOUT);
        send.block(TIMEOUT);
        assertCounts(2, 0, 0, 0);
    }

    @Test
    void checksClosedSessionAtSubscriptionAndCountsNullSeparatelyFromSuccess() {
        Mono<Void> send = sender.sendPayload(session, "payload");
        when(session.isOpen()).thenReturn(false);
        send.block(TIMEOUT);
        sender.sendPayload(null, "payload").block(TIMEOUT);
        assertCounts(0, 0, 2, 0);
        verify(session, never()).send(any());
    }

    @Test
    void recoveredAsyncFailureIsNotSuccessfulSend() {
        when(session.send(any())).thenReturn(Mono.error(new IllegalStateException("send failed")));
        StepVerifier.create(sender.sendPayload(session, "payload")).expectComplete().verify(TIMEOUT);
        assertCounts(0, 1, 0, 0);
    }

    @Test
    void synchronousSendFailureIsCountedAndRecovered() {
        when(session.send(any())).thenThrow(new IllegalStateException("send creation failed"));
        StepVerifier.create(sender.sendPayload(session, "payload")).expectComplete().verify(TIMEOUT);
        assertCounts(0, 1, 0, 0);
    }

    @Test
    void serializationFailureIsCountedBeforeSessionSend() {
        Object invalid = new Object() {
            public String getValue() { throw new IllegalStateException("serialization failed"); }
        };
        sender.sendPayload(session, invalid).block(TIMEOUT);
        assertCounts(0, 1, 0, 0);
        verify(session, never()).send(any());
    }

    @Test
    void pendingSendCancellationIsDistinctAndLateCompletionDoesNotCount() {
        Sinks.Empty<Void> completion = Sinks.empty();
        when(session.send(any())).thenReturn(completion.asMono());
        StepVerifier.create(sender.sendPayload(session, "payload"))
                .then(() -> assertCounts(0, 0, 0, 0))
                .thenCancel().verify(TIMEOUT);
        assertCounts(0, 0, 0, 1);
        completion.tryEmitEmpty();
        assertCounts(0, 0, 0, 1);
    }

    @Test
    void resetClearsAllOutcomes() {
        recorder.recordSent();
        recorder.recordFailed();
        recorder.recordSkipped();
        recorder.recordCancelled();
        assertCounts(1, 1, 1, 1);
        recorder.reset();
        assertCounts(0, 0, 0, 0);
        assertTrue(recorder.snapshot().perSecond().isEmpty());
    }

    @Test
    void endpointExposesOutcomesAndDeleteResetsThem() {
        WebTestClient client = WebTestClient.bindToRouterFunction(
                new ThroughputMetricsRouter().routeThroughputMetrics(recorder)).build();
        recorder.recordSent();
        recorder.recordFailed();
        recorder.recordSkipped();
        recorder.recordCancelled();
        client.get().uri("/internal/metrics/throughput").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.totalSent").isEqualTo(1)
                .jsonPath("$.totalFailed").isEqualTo(1)
                .jsonPath("$.totalSkipped").isEqualTo(1)
                .jsonPath("$.totalCancelled").isEqualTo(1)
                .jsonPath("$.seconds").isEqualTo(0)
                .jsonPath("$.maxPerSec").isEqualTo(0)
                .jsonPath("$.perSecond").isArray();
        client.delete().uri("/internal/metrics/throughput").exchange().expectStatus().isNoContent();
        assertCounts(0, 0, 0, 0);
    }

    @Test
    void disabledRecorderPreservesSuccessSkipFailureAndCancellation() {
        sender = sender(false);
        sender.sendPayload(session, "payload").block(TIMEOUT);
        sender.sendPayload(null, "payload").block(TIMEOUT);
        when(session.send(any())).thenReturn(Mono.error(new IllegalStateException("send failed")));
        sender.sendPayload(session, "payload").block(TIMEOUT);
        when(session.send(any())).thenReturn(Mono.never());
        StepVerifier.create(sender.sendPayload(session, "payload")).thenCancel().verify(TIMEOUT);
        assertCounts(0, 0, 0, 0);
    }

    @SuppressWarnings("unchecked")
    private MessageSender sender(boolean enabled) {
        ObjectProvider<ThroughputRecorder> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(enabled ? recorder : null);
        return new MessageSender(new ObjectMapper(), mock(SessionManager.class), provider);
    }

    private void assertCounts(long sent, long failed, long skipped, long cancelled) {
        ThroughputRecorder.Snapshot snapshot = recorder.snapshot();
        assertAll(() -> assertEquals(sent, snapshot.totalSent()),
                () -> assertEquals(failed, snapshot.totalFailed()),
                () -> assertEquals(skipped, snapshot.totalSkipped()),
                () -> assertEquals(cancelled, snapshot.totalCancelled()));
    }
}
