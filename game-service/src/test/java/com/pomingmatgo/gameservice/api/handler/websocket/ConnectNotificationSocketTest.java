package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.application.room.RoomService;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicReference;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// 실제 전송 후 완료 신호만 지연한다. TCP backpressure나 외부 클라이언트 적용 재현은 아니다.
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@ActiveProfiles("in-memory")
class ConnectNotificationSocketTest {
    private static final long ROOM_ID = 960_094L;
    private static final long USER_1 = 101L;
    private static final long USER_2 = 102L;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Autowired GameWebSocketHandler handler;
    @Autowired GameStateRepository states;
    @Autowired SessionManager sessions;
    @Autowired RoomCleanupService cleanup;
    @Autowired RoomService rooms;
    @Autowired ObjectMapper mapper;

    private final List<TestSession> clients = new CopyOnWriteArrayList<>();
    private DisposableServer server;
    private TestSession first;
    private TestSession second;
    private final Sinks.Empty<Void> releaseSend = Sinks.empty();

    @BeforeEach
    void setUp() throws Exception {
        rooms.createRoom(ROOM_ID).block(TIMEOUT);
        rooms.joinRoom(USER_1, ROOM_ID).block(TIMEOUT);
        rooms.joinRoom(USER_2, ROOM_ID).block(TIMEOUT);
        HandshakeWebSocketService upgrade = new HandshakeWebSocketService();
        var httpHandler = WebHttpHandlerBuilder.webHandler(exchange -> upgrade.handleRequest(exchange, session -> {
            TestSession client = clients.get(Integer.parseInt(exchange.getRequest().getURI().getPath().substring(1)));
            WebSocketSession observed = mock(WebSocketSession.class, org.mockito.AdditionalAnswers.delegatesTo(session));
            doAnswer(invocation -> Mono.defer(() -> {
                AtomicBoolean hold = new AtomicBoolean();
                Publisher<WebSocketMessage> source = invocation.getArgument(0);
                return session.send(Flux.from(source).doOnNext(message -> {
                    JsonNode node = parse(message.getPayloadAsText());
                    client.entered.add(node);
                    String target = client.holdStatus.get();
                    if (node.path("status").asText().equals(target)
                            && client.holdStatus.compareAndSet(target, null)) hold.set(true);
                })).then(Mono.defer(() -> hold.get() ? releaseSend.asMono() : Mono.empty()))
                        .doOnError(client.failure::set);
            })).when(observed).send(any());
            client.session.set(observed);
            return handler.handle(observed).doOnError(client.failure::set)
                    .doFinally(ignored -> client.handlerEnded.tryEmitEmpty());
        })).build();
        server = HttpServer.create().host("127.0.0.1").port(0)
                .handle(new ReactorHttpHandlerAdapter(httpHandler)).bindNow(TIMEOUT);
        first = newSession();
        second = newSession();
        first.emit(connectJson(USER_1));
        await(() -> first.count("CONNECT") == 1);
    }

    @AfterEach
    void tearDown() {
        releaseSend.tryEmitEmpty();
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
        try {
            clients.forEach(client -> {
                if (client.connection.get() != null) client.connection.get().dispose();
            });
            clients.forEach(client -> {
                if (client.session.get() != null) client.handlerEnded.asMono().block(TIMEOUT);
            });
        } finally {
            clients.forEach(client -> {
                if (client.subscription != null) client.subscription.dispose();
            });
            if (server != null) server.disposeNow(TIMEOUT);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void connectArrivesBeforePeerReadyEvenWhenConnectCompletionWaits(boolean delayed) throws Exception {
        holdSend(first, "CONNECT", delayed);
        second.emit(connectJson(USER_2));
        await(() -> second.count("CONNECT") == 1);
        await(() -> releaseSend.currentSubscriberCount() == (delayed ? 1 : 0));

        // 각각 자신의 CONNECT 안내 이후 요청한다. 두 번째 연결의 READY는 CONNECT 전체 완료 뒤 처리된다.
        ready(first);
        await(() -> first.count("READY") == 1 && second.count("READY") == 1);
        ready(second);
        if (delayed) {
            assertEquals(GamePhase.NONE, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
            assertEquals(2, first.count("CONNECT"));
            assertEquals(1, second.count("READY"));
            assertEquals(0, first.count("START"));
            assertEquals(0, second.count("START"));
        }

        releaseSend.tryEmitEmpty();
        awaitStarted();
        assertEquals(List.of("CONNECT", "CONNECT", "READY", "READY", "START"),
                statuses(first));
        assertEquals(List.of("CONNECT", "READY", "READY", "START"), statuses(second));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void readyArrivesBeforeConnectEvenWhenReadyCompletionWaits(boolean delayed) throws Exception {
        holdSend(first, "READY", delayed);
        ready(first);
        await(() -> first.count("READY") == 1);
        await(() -> releaseSend.currentSubscriberCount() == (delayed ? 1 : 0));

        // 독립 연결의 CONNECT는 기존 READY 예약을 기다리지 않는다.
        second.emit(connectJson(USER_2));
        await(() -> second.count("CONNECT") == 1);
        await(() -> first.count("CONNECT") == 2);
        ready(second);
        await(() -> states.findById(ROOM_ID).block(TIMEOUT).getPhase() == GamePhase.DETERMINING_STARTING_PLAYER);
        if (delayed) {
            assertEquals(1, first.count("READY"));
            assertEquals(0, second.count("READY"));
            assertEquals(0, first.count("START"));
            assertEquals(0, second.count("START"));
        }

        releaseSend.tryEmitEmpty();
        awaitStarted();
        assertEquals(List.of("CONNECT", "READY", "CONNECT", "READY", "START"),
                statuses(first));
        // 첫 READY 완료 시 아직 등록되지 않은 상대에게 이전 READY를 추가하지 않는다.
        assertEquals(List.of("CONNECT", "READY", "START"), statuses(second));
    }

    private void awaitStarted() throws InterruptedException {
        await(() -> first.count("START") == 1 && second.count("START") == 1);
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        assertSame(first.session.get(), sessions.getSession(ROOM_ID, 1));
        assertSame(second.session.get(), sessions.getSession(ROOM_ID, 2));
        for (TestSession client : clients) {
            assertTrue(client.outbox.stream().noneMatch(node -> node.has("errorCode")));
            assertEquals(0, client.count("RECONNECT_STATE"));
            assertEquals(client.entered, client.outbox, "전송 진입과 실제 수신의 본문·순서·건수");
            assertNull(client.failure.get());
        }
        assertEquals(0, releaseSend.currentSubscriberCount());
    }

    private static List<String> statuses(TestSession client) {
        return client.outbox.stream().map(node -> node.path("status").asText()).toList();
    }

    private void ready(TestSession client) {
        client.emit("{\"eventType\":{\"subType\":\"READY\"}}");
    }

    private void holdSend(TestSession client, String status, boolean delayed) {
        if (delayed) client.holdStatus.set(status);
    }

    private static class TestSession {
        final AtomicReference<WebSocketSession> session = new AtomicReference<>();
        final AtomicReference<Connection> connection = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<String> holdStatus = new AtomicReference<>();
        final Sinks.Many<String> inbound = Sinks.many().unicast().onBackpressureBuffer();
        final Sinks.Empty<Void> handlerEnded = Sinks.empty();
        final List<JsonNode> entered = new CopyOnWriteArrayList<>();
        final List<JsonNode> outbox = new CopyOnWriteArrayList<>();
        Disposable subscription;

        void emit(String json) {
            assertEquals(Sinks.EmitResult.OK, inbound.tryEmitNext(json));
        }

        long count(String status) {
            return outbox.stream().filter(node -> status.equals(node.path("status").asText())).count();
        }
    }

    private TestSession newSession() throws InterruptedException {
        TestSession client = new TestSession();
        int index = clients.size();
        clients.add(client);
        client.subscription = HttpClient.create().host("127.0.0.1").port(server.port()).websocket()
                .uri("/" + index).handle((in, out) -> {
                    in.withConnection(client.connection::set);
                    return Mono.when(out.sendString(client.inbound.asFlux()).then(),
                            in.receive().asString().map(this::parse).doOnNext(client.outbox::add).then());
                }).subscribe(ignored -> {}, client.failure::set);
        await(() -> client.session.get() != null && client.connection.get() != null);
        return client;
    }

    private JsonNode parse(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception error) {
            throw new IllegalArgumentException(json, error);
        }
    }

    private String connectJson(long userId) {
        return "{\"eventType\":{\"subType\":\"CONNECT\"},\"data\":{\"userId\":" + userId
                + ",\"roomId\":" + ROOM_ID + "}}";
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(10);
        }
        fail("기대 상태에 도달하지 못함");
    }
}
