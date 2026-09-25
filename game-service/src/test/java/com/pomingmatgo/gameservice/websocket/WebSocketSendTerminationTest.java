package com.pomingmatgo.gameservice.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.websocket.WebsocketOutbound;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class WebSocketSendTerminationTest {
    private static final long ROOM_ID = 970_001L;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private final SessionManager sessions = new SessionManager();
    private final Peer old = new Peer();
    private final Peer replacement = new Peer();
    private final Sinks.Empty<Void> pendingPayload = Sinks.empty();
    private final Sinks.One<SignalType> payloadEnded = Sinks.one();
    private final Sinks.One<SignalType> sendEnded = Sinks.one();
    private final AtomicReference<Throwable> sendFailure = new AtomicReference<>();
    private DisposableServer server;

    enum Termination { CLOSE_FRAME, CHANNEL_CLOSE, REPLACEMENT }

    @AfterEach
    void tearDown() {
        for (Peer peer : List.of(old, replacement)) {
            if (peer.connection.get() != null) peer.connection.get().dispose();
            if (peer.client != null) peer.client.dispose();
        }
        pendingPayload.tryEmitEmpty();
        if (server != null) server.disposeNow(TIMEOUT);
        sessions.shutdown();
    }

    @ParameterizedTest
    @EnumSource(Termination.class)
    void closingOldConnectionReleasesPendingSend(Termination termination) {
        startServer();
        connect(old, "/old");
        assertEquals("before-close", old.firstMessage.asMono().block(TIMEOUT));
        old.payloadSubscribed.asMono().block(TIMEOUT);
        assertEquals(1, pendingPayload.currentSubscriberCount());

        switch (termination) {
            case CLOSE_FRAME -> old.outbound.get().sendClose().block(TIMEOUT);
            case CHANNEL_CLOSE -> old.connection.get().channel().close();
            case REPLACEMENT -> {
                connect(replacement, "/replacement");
                replacement.registered.asMono().block(TIMEOUT);
            }
        }

        assertEquals(SignalType.CANCEL, payloadEnded.asMono().block(TIMEOUT));
        SignalType sendTermination = sendEnded.asMono().block(TIMEOUT);
        assertTrue(List.of(SignalType.ON_COMPLETE, SignalType.ON_ERROR, SignalType.CANCEL)
                .contains(sendTermination));
        if (sendTermination == SignalType.ON_ERROR) assertNotNull(sendFailure.get());
        old.handlerEnded.asMono().block(TIMEOUT);
        old.clientEnded.asMono().block(TIMEOUT);
        assertEquals(0, pendingPayload.currentSubscriberCount());
        assertFalse(old.session.get().isOpen());
        assertEquals(List.of("before-close"), old.messages);

        if (termination == Termination.REPLACEMENT) {
            WebSocketSession current = replacement.session.get();
            assertSame(current, sessions.getSession(ROOM_ID, 1));
            assertNull(sessions.getPlayerContext(old.session.get()).block(TIMEOUT));
            // 이전 연결의 늦은 disconnect가 도착한 뒤에도 새 연결로 실제 왕복한다.
            sessions.deletePlayer(ROOM_ID, 1, old.session.get());
            assertSame(current, sessions.getSession(ROOM_ID, 1));
            replacement.outbound.get().sendString(Mono.just("after-replacement")).then().block(TIMEOUT);
            assertEquals("\"after-replacement\"", replacement.firstMessage.asMono().block(TIMEOUT));
            assertTrue(current.isOpen());
            assertEquals(new SessionManager.PlayerContext(ROOM_ID, 101L, 1),
                    sessions.getPlayerContext(current).block(TIMEOUT));
            assertNull(replacement.failure.get());
        }
        assertNull(old.failure.get());
    }

    private void startServer() {
        @SuppressWarnings("unchecked")
        ObjectProvider<ThroughputRecorder> provider = mock(ObjectProvider.class);
        MessageSender sender = new MessageSender(new ObjectMapper(), sessions, provider);
        HandshakeWebSocketService upgrade = new HandshakeWebSocketService();
        var handler = WebHttpHandlerBuilder.webHandler(exchange -> {
            Peer peer = exchange.getRequest().getPath().value().equals("/old") ? old : replacement;
            return upgrade.handleRequest(exchange, session -> {
                peer.session.set(session);
                Mono<Void> receive = session.receive()
                        .concatMap(message -> sender.sendPayload(session, message.getPayloadAsText())).then();
                // 실제 send의 원본 Publisher만 열어 둔다. 느린 TCP 수신을 모사하지 않는다.
                Mono<Void> pendingSend = Mono.defer(() -> session.send(Flux.concat(
                                Mono.just(session.textMessage("before-close")),
                                pendingPayload.asMono()
                                        .doOnRequest(ignored -> peer.payloadSubscribed.tryEmitEmpty())
                                        .thenMany(Flux.empty()))
                        .doFinally(payloadEnded::tryEmitValue)))
                        .doOnError(sendFailure::set)
                        .doFinally(sendEnded::tryEmitValue);
                return sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 101L, session)
                        .then(Mono.fromRunnable(() -> peer.registered.tryEmitEmpty()))
                        .then(peer == old ? Mono.when(receive, pendingSend) : receive)
                        .doFinally(peer.handlerEnded::tryEmitValue);
            });
        }).build();
        server = HttpServer.create().host("127.0.0.1").port(0)
                .handle(new ReactorHttpHandlerAdapter(handler)).bindNow(TIMEOUT);
    }

    private void connect(Peer peer, String path) {
        peer.client = HttpClient.create().host("127.0.0.1").port(server.port()).websocket()
                .uri(path).handle((in, out) -> {
                    in.withConnection(peer.connection::set);
                    peer.outbound.set(out);
                    peer.clientReady.tryEmitEmpty();
                    return in.receive().asString().doOnNext(message -> {
                        peer.messages.add(message);
                        peer.firstMessage.tryEmitValue(message);
                    }).then();
                }).doFinally(ignored -> peer.clientEnded.tryEmitEmpty())
                .subscribe(ignored -> {}, peer.failure::set);
        peer.clientReady.asMono().block(TIMEOUT);
    }

    private static final class Peer {
        final AtomicReference<Connection> connection = new AtomicReference<>();
        final AtomicReference<WebsocketOutbound> outbound = new AtomicReference<>();
        final AtomicReference<WebSocketSession> session = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Sinks.Empty<Void> registered = Sinks.empty();
        final Sinks.Empty<Void> clientReady = Sinks.empty();
        final Sinks.Empty<Void> payloadSubscribed = Sinks.empty();
        final Sinks.Empty<Void> clientEnded = Sinks.empty();
        final Sinks.One<String> firstMessage = Sinks.one();
        final Sinks.One<SignalType> handlerEnded = Sinks.one();
        final List<String> messages = new CopyOnWriteArrayList<>();
        Disposable client;
    }
}
