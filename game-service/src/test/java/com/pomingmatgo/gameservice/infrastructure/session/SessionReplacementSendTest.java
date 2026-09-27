package com.pomingmatgo.gameservice.infrastructure.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.infrastructure.messaging.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SessionReplacementSendTest {
    private static final long ROOM_ID = 19L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final SessionManager sessions = new SessionManager();
    private final Disposable.Composite sends = Disposables.composite();
    private final List<String> oldPayloads = new ArrayList<>();
    private final List<String> currentPayloads = new ArrayList<>();
    private final Sinks.Empty<Void> oldSend = Sinks.empty();
    private final Sinks.Empty<Void> currentSend = Sinks.empty();
    private WebSocketSession old;
    private WebSocketSession current;
    private MessageSender sender;

    @BeforeEach
    void setUp() {
        old = session("old", oldPayloads, oldSend);
        current = session("current", currentPayloads, currentSend);
        @SuppressWarnings("unchecked")
        ObjectProvider<ThroughputRecorder> provider = mock(ObjectProvider.class);
        sender = new MessageSender(new ObjectMapper(), sessions, provider);
        complete(sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 101L, old));
    }

    @AfterEach
    void tearDown() {
        sends.dispose();
        sessions.shutdown();
    }

    enum Outcome { COMPLETE, ERROR, CANCEL }

    @ParameterizedTest
    @EnumSource(Outcome.class)
    void oldSendTerminationDoesNotTerminateReplacementSendOrRemoveItsMapping(Outcome outcome) {
        Disposable previous = sender.sendPayload(old, "previous").subscribe();
        sends.add(previous);
        assertEquals(1, oldSend.currentSubscriberCount());

        replace();
        sends.add(sender.sendPayload(current, "new").subscribe());
        assertEquals(1, currentSend.currentSubscriberCount());
        // close 완료와 송신 완료는 테스트 더블에서 별도로 제어한다.
        assertEquals(1, oldSend.currentSubscriberCount());

        switch (outcome) {
            case COMPLETE -> assertEquals(Sinks.EmitResult.OK, oldSend.tryEmitEmpty());
            case ERROR -> assertEquals(Sinks.EmitResult.OK,
                    oldSend.tryEmitError(new IllegalStateException("old send failed")));
            case CANCEL -> previous.dispose();
        }
        sessions.deletePlayer(ROOM_ID, 1, old);

        assertEquals(0, oldSend.currentSubscriberCount());
        assertEquals(1, currentSend.currentSubscriberCount());
        assertCurrentMapping();
        assertEquals(List.of("\"previous\""), oldPayloads);
        assertEquals(List.of("\"new\""), currentPayloads);
        verify(current, never()).close();
        assertEquals(Sinks.EmitResult.OK, currentSend.tryEmitEmpty());
        assertEquals(0, currentSend.currentSubscriberCount());
    }

    @Test
    void broadcastCreatedBeforeReplacementResolvesRecipientsOnSubscription() {
        Mono<Void> broadcast = broadcast("after replacement");
        replace();

        StepVerifier.create(broadcast)
                .then(() -> {
                    assertTrue(oldPayloads.isEmpty());
                    assertEquals(1, currentPayloads.size());
                    assertTrue(currentPayloads.getFirst().contains("after replacement"));
                    assertEquals(1, currentSend.currentSubscriberCount());
                    assertEquals(Sinks.EmitResult.OK, currentSend.tryEmitEmpty());
                })
                .expectComplete().verify(TIMEOUT);
        verify(old, never()).send(any());
        assertCurrentMapping();
    }

    @Test
    void subscribedBroadcastStaysWithOldSessionWhileNextBroadcastUsesReplacement() {
        sends.add(broadcast("before replacement").subscribe());
        replace();
        sends.add(broadcast("after replacement").subscribe());

        assertEquals(1, oldPayloads.size());
        assertTrue(oldPayloads.getFirst().contains("before replacement"));
        assertEquals(1, currentPayloads.size());
        assertTrue(currentPayloads.getFirst().contains("after replacement"));
        assertEquals(1, oldSend.currentSubscriberCount());
        assertEquals(1, currentSend.currentSubscriberCount());
        assertEquals(Sinks.EmitResult.OK, currentSend.tryEmitEmpty());
        assertEquals(1, oldSend.currentSubscriberCount());
        assertEquals(Sinks.EmitResult.OK, oldSend.tryEmitEmpty());
        assertCurrentMapping();
        verify(current, times(1)).send(any());
    }

    private WebSocketSession session(String id, List<String> payloads, Sinks.Empty<Void> gate) {
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn(id);
        when(session.isOpen()).thenReturn(true);
        when(session.close()).thenReturn(Mono.empty());
        when(session.textMessage(anyString())).thenAnswer(call -> new WebSocketMessage(
                WebSocketMessage.Type.TEXT, DefaultDataBufferFactory.sharedInstance.wrap(
                        call.<String>getArgument(0).getBytes(StandardCharsets.UTF_8))));
        when(session.send(any())).thenAnswer(call -> Flux
                .from(call.<org.reactivestreams.Publisher<WebSocketMessage>>getArgument(0))
                .doOnNext(message -> payloads.add(message.getPayloadAsText()))
                .then(gate.asMono()));
        return session;
    }

    private void replace() {
        complete(sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 101L, current));
        verify(old).close();
        assertCurrentMapping();
    }

    private Mono<Void> broadcast(String message) {
        return sender.sendMessageToAllUser(ROOM_ID, new WebSocketResDto<>(Player.PLAYER_1, null, message));
    }

    private void assertCurrentMapping() {
        assertSame(current, sessions.getSession(ROOM_ID, 1));
        StepVerifier.create(sessions.getPlayerContext(old)).expectComplete().verify(TIMEOUT);
        StepVerifier.create(sessions.getPlayerContext(current))
                .expectNext(new SessionManager.PlayerContext(ROOM_ID, 101L, 1))
                .expectComplete().verify(TIMEOUT);
    }

    private void complete(Mono<Void> operation) {
        StepVerifier.create(operation).expectComplete().verify(TIMEOUT);
    }
}
