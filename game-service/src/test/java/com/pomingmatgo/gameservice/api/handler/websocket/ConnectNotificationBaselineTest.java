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
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// 일반 CONNECT와 READY의 제어 송신 기준선이다. 실제 TCP 수신·적용 순서와 구분한다.
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@ActiveProfiles("in-memory")
class ConnectNotificationBaselineTest {
    private static final long ROOM_ID = 960_093L;
    private static final long USER_1 = 101L;
    private static final long USER_2 = 102L;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Autowired GameWebSocketHandler handler;
    @Autowired GameStateRepository states;
    @Autowired SessionManager sessions;
    @Autowired RoomCleanupService cleanup;
    @Autowired RoomService rooms;
    @Autowired ObjectMapper mapper;

    private final List<TestSession> clients = new ArrayList<>();
    private TestSession first;
    private TestSession second;
    private final Sinks.Empty<Void> releaseSend = Sinks.empty();

    @BeforeEach
    void setUp() throws Exception {
        rooms.createRoom(ROOM_ID).block(TIMEOUT);
        rooms.joinRoom(USER_1, ROOM_ID).block(TIMEOUT);
        rooms.joinRoom(USER_2, ROOM_ID).block(TIMEOUT);
        first = newSession("connect-first");
        second = newSession("connect-second");
        first.emit(connectJson(USER_1));
        await(() -> first.count("CONNECT") == 1);
    }

    @AfterEach
    void tearDown() {
        releaseSend.tryEmitEmpty();
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
        clients.forEach(client -> client.subscription().dispose());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void peerReadyCanOvertakeConnectButStartWaitsForConnectingRequestBaseline(boolean delayed) throws Exception {
        holdSend(first, "CONNECT", delayed);
        second.emit(connectJson(USER_2));
        await(() -> second.count("CONNECT") == 1);
        assertEquals(delayed ? 1 : 0, releaseSend.currentSubscriberCount());

        // 각각 자신의 CONNECT 안내 이후 요청한다. 두 번째 연결의 READY는 CONNECT 전체 완료 뒤 처리된다.
        ready(first);
        await(() -> first.count("READY") == 1);
        ready(second);
        if (delayed) {
            assertEquals(GamePhase.NONE, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
            assertEquals(1, first.count("CONNECT"));
            assertEquals(1, second.count("READY"));
            assertEquals(0, first.count("START"));
            assertEquals(0, second.count("START"));
        }

        releaseSend.tryEmitEmpty();
        awaitStarted();
        assertEquals(delayed
                        ? List.of("CONNECT", "READY", "CONNECT", "READY", "START")
                        : List.of("CONNECT", "CONNECT", "READY", "READY", "START"),
                statuses(first));
        assertEquals(List.of("CONNECT", "READY", "READY", "START"), statuses(second));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void connectCanOvertakeEarlierReadyButStartWaitsForReadyReservationBaseline(boolean delayed) throws Exception {
        holdSend(first, "READY", delayed);
        ready(first);
        assertEquals(delayed ? 1 : 0, releaseSend.currentSubscriberCount());

        // 독립 연결의 CONNECT는 기존 READY 예약을 기다리지 않는다.
        second.emit(connectJson(USER_2));
        await(() -> second.count("CONNECT") == 1);
        assertEquals(2, first.count("CONNECT"));
        ready(second);
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        if (delayed) {
            assertEquals(0, first.count("READY"));
            assertEquals(0, second.count("READY"));
            assertEquals(0, first.count("START"));
            assertEquals(0, second.count("START"));
        }

        releaseSend.tryEmitEmpty();
        awaitStarted();
        assertEquals(delayed
                        ? List.of("CONNECT", "CONNECT", "READY", "READY", "START")
                        : List.of("CONNECT", "READY", "CONNECT", "READY", "START"),
                statuses(first));
        // 첫 READY 완료 시 아직 등록되지 않은 상대에게 이전 READY를 추가하지 않는다.
        assertEquals(List.of("CONNECT", "READY", "START"), statuses(second));
    }

    private void awaitStarted() throws InterruptedException {
        await(() -> first.count("START") == 1 && second.count("START") == 1);
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        assertSame(first.session(), sessions.getSession(ROOM_ID, 1));
        assertSame(second.session(), sessions.getSession(ROOM_ID, 2));
        for (TestSession client : clients) {
            assertTrue(client.outbox().stream().noneMatch(node -> node.has("errorCode")));
            assertEquals(0, client.count("RECONNECT_STATE"));
        }
        assertEquals(0, releaseSend.currentSubscriberCount());
    }

    private static List<String> statuses(TestSession client) {
        return client.outbox().stream().map(node -> node.path("status").asText()).toList();
    }

    private void ready(TestSession client) {
        client.emit("{\"eventType\":{\"subType\":\"READY\"}}");
    }

    private void holdSend(TestSession client, String status, boolean delayed) {
        AtomicBoolean held = new AtomicBoolean();
        doAnswer(invocation -> Flux.from(invocation.<Publisher<WebSocketMessage>>getArgument(0))
                .concatMap(message -> Mono.fromCallable(() -> mapper.readTree(message.getPayloadAsText())))
                .concatMap(node -> {
                    Mono<Void> wait = delayed && status.equals(node.path("status").asText())
                            && held.compareAndSet(false, true) ? releaseSend.asMono() : Mono.empty();
                    return wait.then(Mono.fromRunnable(() -> client.outbox().add(node)));
                }).then()).when(client.session()).send(any());
    }

    private record TestSession(WebSocketSession session, Sinks.Many<WebSocketMessage> inbound,
                               List<JsonNode> outbox, Disposable subscription) {
        void emit(String json) {
            assertEquals(Sinks.EmitResult.OK, inbound.tryEmitNext(text(json)));
        }

        long count(String status) {
            return outbox.stream().filter(node -> status.equals(node.path("status").asText())).count();
        }
    }

    private TestSession newSession(String id) {
        WebSocketSession session = mock(WebSocketSession.class);
        Sinks.Many<WebSocketMessage> inbound = Sinks.many().unicast().onBackpressureBuffer();
        List<JsonNode> outbox = new CopyOnWriteArrayList<>();
        when(session.getId()).thenReturn(id);
        when(session.isOpen()).thenReturn(true);
        when(session.receive()).thenReturn(inbound.asFlux());
        when(session.close()).thenReturn(Mono.fromRunnable(inbound::tryEmitComplete));
        when(session.textMessage(anyString())).thenAnswer(invocation -> text(invocation.getArgument(0)));
        when(session.send(any())).thenAnswer(invocation -> Flux.from(
                        invocation.<Publisher<WebSocketMessage>>getArgument(0))
                .concatMap(message -> Mono.fromCallable(() -> mapper.readTree(message.getPayloadAsText())))
                .doOnNext(outbox::add).then());
        Disposable subscription = handler.handle(session).subscribe();
        TestSession client = new TestSession(session, inbound, outbox, subscription);
        clients.add(client);
        return client;
    }

    private String connectJson(long userId) {
        return "{\"eventType\":{\"subType\":\"CONNECT\"},\"data\":{\"userId\":" + userId
                + ",\"roomId\":" + ROOM_ID + "}}";
    }

    private static WebSocketMessage text(String payload) {
        return new WebSocketMessage(WebSocketMessage.Type.TEXT,
                DefaultDataBufferFactory.sharedInstance.wrap(payload.getBytes(StandardCharsets.UTF_8)));
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
