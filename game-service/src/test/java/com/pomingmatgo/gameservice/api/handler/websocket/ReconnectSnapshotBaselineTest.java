package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryInstalledCardRepository;
import com.pomingmatgo.gameservice.infrastructure.scheduler.AutoPlayScheduler;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// 현재 동작의 기준선이다. 조회 혼합과 늦은 스냅샷을 해결하면 기대값도 정상 보장으로 전환한다.
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@ActiveProfiles("in-memory")
class ReconnectSnapshotBaselineTest {
    private static final long ROOM_ID = 960_071L;
    private static final long USER_1 = 101L;
    private static final long USER_2 = 102L;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Autowired GameWebSocketHandler handler;
    @Autowired GameStateRepository states;
    @Autowired SessionManager sessions;
    @Autowired RoomCleanupService cleanup;
    @Autowired AutoPlayScheduler autoPlay;
    @Autowired ObjectMapper mapper;
    @SpyBean InMemoryInstalledCardRepository cards;

    private final ExecutorService reconnectWorker = Executors.newSingleThreadExecutor();
    private final CountDownLatch paused = new CountDownLatch(1);
    private final CountDownLatch resume = new CountDownLatch(1);
    private final List<TestSession> clients = new ArrayList<>();
    private TestSession opponent;

    @BeforeEach
    void setUp() throws Exception {
        states.create(GameState.createEmptyRoom(ROOM_ID).join(USER_1).join(USER_2).toBuilder()
                .leadingPlayer(1).currentTurn(1).round(1).phase(GamePhase.IN_PROGRESS).build()).block(TIMEOUT);
        List<Card> hand1 = List.of(Card.JAN_1, Card.FEB_1, Card.MAR_1, Card.APR_1, Card.MAY_2,
                Card.JUN_2, Card.JUL_2, Card.AUG_1, Card.SEP_1, Card.OCT_1);
        List<Card> hand2 = List.of(Card.JAN_2, Card.FEB_2, Card.MAR_2, Card.APR_2, Card.MAY_3,
                Card.JUN_3, Card.JUL_3, Card.AUG_2, Card.SEP_2, Card.NOV_1);
        List<Card> floor = List.of(Card.MAY_1, Card.JUN_1, Card.AUG_3, Card.SEP_3,
                Card.OCT_2, Card.NOV_2, Card.DEC_1, Card.DEC_2);
        List<Card> deck = new ArrayList<>(List.of(Card.JUL_1));
        Arrays.stream(Card.values()).filter(card -> !hand1.contains(card) && !hand2.contains(card)
                && !floor.contains(card) && card != Card.JUL_1).forEach(deck::add);
        assertEquals(20, deck.size());
        cards.savePlayerCards(hand1, ROOM_ID, Player.PLAYER_1).block(TIMEOUT);
        cards.savePlayerCards(hand2, ROOM_ID, Player.PLAYER_2).block(TIMEOUT);
        cards.saveRevealedCard(floor, ROOM_ID).block(TIMEOUT);
        cards.saveHiddenCard(deck, ROOM_ID).block(TIMEOUT);

        opponent = newSession("opponent", false);
        opponent.emit(connectJson(USER_1));
        TestSession previous = newSession("previous", false);
        previous.emit(connectJson(USER_2));
        assertEquals(1, previous.count("RECONNECT_STATE"));
        previous.drop();
        await(() -> sessions.getSession(ROOM_ID, 2) == null
                && opponent.count("OPPONENT_DISCONNECTED") == 1);
        assertNotNull(states.findById(ROOM_ID).block(TIMEOUT));
    }

    @AfterEach
    void tearDown() throws Exception {
        resume.countDown();
        reconnectWorker.shutdown();
        try {
            assertTrue(reconnectWorker.awaitTermination(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        } finally {
            reconnectWorker.shutdownNow();
            cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
            clients.forEach(client -> client.subscription().dispose());
        }
    }

    @ParameterizedTest(name = "조회 사이 정지={0}, 자동플레이={1}")
    @CsvSource({"true, false", "true, true", "false, false", "false, true"})
    void reconnectCanPublishAnOldTurnAfterAnIndependentAction(boolean pauseBetweenReads, boolean autoplay)
            throws Exception {
        if (pauseBetweenReads) {
            // 동기 조회 사이의 스레드 선점을 고정한다. 메모리 저장소의 I/O 지연을 가정하지 않는다.
            doAnswer(invocation -> {
                pauseHere();
                return invocation.callRealMethod();
            }).when(cards).getPlayerCards(ROOM_ID, Player.PLAYER_2);
        }
        TestSession reconnecting = newSession("reconnecting", !pauseBetweenReads);
        var connect = reconnectWorker.submit(() -> reconnecting.emit(connectJson(USER_2)));
        assertTrue(paused.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "재접속 경계에 도달하지 못함");
        assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));

        if (autoplay) {
            autoPlay.scheduleAutoPlay(ROOM_ID, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.IN_PROGRESS);
        } else {
            opponent.emit("{\"eventType\":{\"subType\":\"NORMAL_SUBMIT\"},\"data\":{\"cardIndex\":0}}");
        }
        await(() -> reconnecting.count("ANNOUNCE_TURN_INFORMATION") == 1);
        assertEquals(0, reconnecting.count("RECONNECT_STATE"));
        assertEquals(2, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
        assertEquals(9, cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).size());
        assertEquals(10, cards.getAllRevealedCards(ROOM_ID).block(TIMEOUT).size());

        resume.countDown();
        connect.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        JsonNode snapshot = reconnecting.data("RECONNECT_STATE");
        assertEquals(1, snapshot.path("currentTurn").asInt());
        assertEquals("PLAYER_1", snapshot.path("currentPlayer").asText());
        assertEquals(10, snapshot.path("myCards").size());
        assertEquals(pauseBetweenReads ? 9 : 10, snapshot.path("opponentCardCount").asInt());
        assertEquals(pauseBetweenReads, snapshot.path("floorCards").has("1"));
        assertEquals(pauseBetweenReads, snapshot.path("floorCards").has("7"));
        assertEquals("PLAYER_2", reconnecting.data("ANNOUNCE_TURN_INFORMATION").path("curPlayer").asText());
        assertEquals(2, reconnecting.data("ANNOUNCE_TURN_INFORMATION").path("turn").asInt());
        assertEquals("RECONNECT_STATE", reconnecting.outbox().getLast().path("status").asText());
        assertEquals(1, reconnecting.count("SUBMIT_CARD"));
        assertEquals(1, reconnecting.count("CARD_REVEALED"));
        assertEquals(1, reconnecting.count("RECONNECT_STATE"));
        assertTrue(reconnecting.outbox().stream().noneMatch(node -> node.has("errorCode")));
        assertSame(opponent.session(), sessions.getSession(ROOM_ID, 1));
        assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
        assertFalse(reconnecting.subscription().isDisposed());
    }

    @Test
    void reconnectAfterCompletedActionReadsTheNewTurnAndCards() throws Exception {
        opponent.emit("{\"eventType\":{\"subType\":\"NORMAL_SUBMIT\"},\"data\":{\"cardIndex\":0}}");
        await(() -> opponent.count("ANNOUNCE_TURN_INFORMATION") == 1);
        TestSession reconnecting = newSession("after-action", false);
        reconnecting.emit(connectJson(USER_2));

        JsonNode snapshot = reconnecting.data("RECONNECT_STATE");
        assertEquals(2, snapshot.path("currentTurn").asInt());
        assertEquals("PLAYER_2", snapshot.path("currentPlayer").asText());
        assertEquals(10, snapshot.path("myCards").size());
        assertEquals(9, snapshot.path("opponentCardCount").asInt());
        assertTrue(snapshot.path("floorCards").has("1"));
        assertTrue(snapshot.path("floorCards").has("7"));
    }

    private void pauseHere() {
        paused.countDown();
        try {
            if (!resume.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("테스트 재접속 경계 해제 시간 초과");
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(error);
        }
    }

    private record TestSession(WebSocketSession session, Sinks.Many<WebSocketMessage> inbound,
                               List<JsonNode> outbox, Disposable subscription) {
        void emit(String json) {
            assertEquals(Sinks.EmitResult.OK, inbound.tryEmitNext(text(json)));
        }

        void drop() {
            assertEquals(Sinks.EmitResult.OK, inbound.tryEmitComplete());
        }

        long count(String status) {
            return outbox.stream().filter(node -> status.equals(node.path("status").asText())).count();
        }

        JsonNode data(String status) {
            return outbox.stream().filter(node -> status.equals(node.path("status").asText()))
                    .findFirst().orElseThrow().path("data");
        }
    }

    private TestSession newSession(String id, boolean pauseSnapshotSend) {
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
                .doOnNext(node -> {
                    if (pauseSnapshotSend && "RECONNECT_STATE".equals(node.path("status").asText())) pauseHere();
                    outbox.add(node);
                }).then());
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
