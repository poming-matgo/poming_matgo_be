package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.infrastructure.messaging.MessageSender;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WebSocketConcurrentSendTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final int PRODUCERS = 3;
    private static final int MESSAGES = 32;
    private final SessionManager sessions = new SessionManager();
    private final ThroughputRecorder recorder = new ThroughputRecorder();
    private final Scheduler producers = Schedulers.newParallel("concurrent-send", PRODUCERS);
    private final AtomicReference<Connection> connection = new AtomicReference<>();
    private final AtomicReference<Throwable> clientFailure = new AtomicReference<>();
    private final AtomicReference<Throwable> heldFailure = new AtomicReference<>();
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final Sinks.One<WebSocketSession> connected = Sinks.one();
    private final Sinks.Empty<Void> heldTail = Sinks.empty();
    private final Sinks.Empty<Void> heldSubscribed = Sinks.empty();
    private final Sinks.Empty<Void> markerReceived = Sinks.empty();
    private final Sinks.Empty<Void> clientEnded = Sinks.empty();
    private final Sinks.Empty<Void> handlerEnded = Sinks.empty();
    private final Sinks.Empty<Void> heldEnded = Sinks.empty();
    private DisposableServer server;
    private Disposable client;
    private Disposable held;

    @AfterEach
    void tearDown() {
        if (held != null) held.dispose();
        if (connection.get() != null) connection.get().dispose();
        if (client != null) client.dispose();
        if (server != null) server.disposeNow(TIMEOUT);
        producers.dispose();
        sessions.shutdown();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentMessagesArriveOnceInEachProducerOrder(boolean keepAnotherSendOpen) {
        WebSocketSession session = connect();
        @SuppressWarnings("unchecked")
        ObjectProvider<ThroughputRecorder> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(recorder);
        MessageSender sender = new MessageSender(new ObjectMapper(), sessions, provider);

        if (keepAnotherSendOpen) {
            // 다른 send 구독이 살아 있음을 보장한다. TCP 쓰기 지연을 재현하는 장치는 아니다.
            held = session.send(Flux.concat(Mono.just(session.textMessage("\"held\"")),
                            heldTail.asMono().doOnRequest(ignored -> heldSubscribed.tryEmitEmpty())
                                    .thenMany(Flux.empty())))
                    .doFinally(ignored -> heldEnded.tryEmitEmpty())
                    .subscribe(ignored -> {}, heldFailure::set);
            heldSubscribed.asMono().block(TIMEOUT);
            assertEquals(1, heldTail.currentSubscriberCount());
        }

        List<Mono<Void>> sends = IntStream.range(0, PRODUCERS)
                .mapToObj(producer -> Flux.range(0, MESSAGES)
                        .concatMap(index -> sender.sendPayload(session, producer + ":" + index))
                        .then().subscribeOn(producers))
                .toList();
        Mono.when(sends).block(TIMEOUT);
        if (keepAnotherSendOpen) {
            assertEquals(1, heldTail.currentSubscriberCount());
            assertNull(heldFailure.get());
            assertEquals(Sinks.EmitResult.OK, heldTail.tryEmitEmpty());
            heldEnded.asMono().block(TIMEOUT);
            assertEquals(0, heldTail.currentSubscriberCount());
        }
        sender.sendPayload(session, "end").block(TIMEOUT);
        markerReceived.asMono().block(TIMEOUT);
        session.close().block(TIMEOUT);
        clientEnded.asMono().block(TIMEOUT);
        handlerEnded.asMono().block(TIMEOUT);

        List<String> expected = new ArrayList<>();
        if (keepAnotherSendOpen) expected.add("\"held\"");
        for (int producer = 0; producer < PRODUCERS; producer++) {
            String prefix = "\"" + producer + ":";
            List<String> inOrder = IntStream.range(0, MESSAGES)
                    .mapToObj(index -> prefix + index + "\"").toList();
            expected.addAll(inOrder);
            assertEquals(inOrder, received.stream().filter(value -> value.startsWith(prefix)).toList());
        }
        expected.add("\"end\"");
        assertEquals(expected.size(), received.size(), "누락 또는 추가 메시지");
        assertEquals(new HashSet<>(expected), new HashSet<>(received), "메시지 identity와 중복 확인");
        assertEquals("\"end\"", received.getLast());
        assertNull(clientFailure.get());
        assertNull(heldFailure.get());
        assertFalse(session.isOpen());
        var outcome = recorder.snapshot();
        assertEquals(PRODUCERS * MESSAGES + 1, outcome.totalSent());
        assertEquals(0, outcome.totalFailed(), "MessageSender가 복구한 송신 오류도 없어야 한다");
        assertEquals(0, outcome.totalSkipped());
        assertEquals(0, outcome.totalCancelled());
    }

    private WebSocketSession connect() {
        HandshakeWebSocketService upgrade = new HandshakeWebSocketService();
        var handler = WebHttpHandlerBuilder.webHandler(exchange -> upgrade.handleRequest(exchange, session -> {
            connected.tryEmitValue(session);
            return session.receive().then().doFinally(ignored -> handlerEnded.tryEmitEmpty());
        })).build();
        server = HttpServer.create().host("127.0.0.1").port(0)
                .handle(new ReactorHttpHandlerAdapter(handler)).bindNow(TIMEOUT);
        client = HttpClient.create().host("127.0.0.1").port(server.port()).websocket()
                .uri("/concurrent").handle((in, out) -> {
                    in.withConnection(connection::set);
                    return in.receive().asString().doOnNext(value -> {
                        received.add(value);
                        if (value.equals("\"end\"")) markerReceived.tryEmitEmpty();
                    }).then();
                }).doFinally(ignored -> clientEnded.tryEmitEmpty())
                .subscribe(ignored -> {}, clientFailure::set);
        return connected.asMono().block(TIMEOUT);
    }
}
