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
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// 조회 일관성과 액션 완료 이후 새 수신자 제외는 정상 보장, 늦은 스냅샷은 미해결 기준선이다.
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
    @SpyBean AutoPlayScheduler autoPlay;
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void snapshotReadsExcludeIndependentActionsAndAutoplayCanRetry(boolean autoplay) throws Exception {
        // 동기 조회 사이의 스레드 선점을 고정한다. 메모리 저장소의 I/O 지연을 가정하지 않는다.
        doAnswer(invocation -> {
            pauseHere();
            return invocation.callRealMethod();
        }).when(cards).getPlayerCards(ROOM_ID, Player.PLAYER_2);
        TestSession reconnecting = newSession("reading", false);
        var connect = reconnectWorker.submit(() -> reconnecting.emit(connectJson(USER_2)));
        assertTrue(paused.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));

        if (autoplay) {
            autoPlay.scheduleAutoPlay(ROOM_ID, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.IN_PROGRESS);
            verify(autoPlay, timeout(3000).atLeast(2)).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1),
                    eq(Player.PLAYER_1), anyLong(), eq(GamePhase.IN_PROGRESS));
        } else {
            submit();
            assertEquals("TRY_AGAIN", opponent.outbox().getLast().path("errorCode").asText());
        }
        assertEquals(1, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
        assertEquals(10, cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).size());

        resume.countDown();
        connect.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        JsonNode snapshot = reconnecting.data("RECONNECT_STATE");
        assertEquals(1, snapshot.path("currentTurn").asInt());
        assertEquals("PLAYER_1", snapshot.path("currentPlayer").asText());
        assertEquals(10, snapshot.path("opponentCardCount").asInt());
        assertFalse(snapshot.path("floorCards").has("1"));
        assertFalse(snapshot.path("floorCards").has("7"));
        if (!autoplay) submit();
        await(() -> reconnecting.count("ANNOUNCE_TURN_INFORMATION") == 1);
        assertEquals(2, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
        assertEquals(1, reconnecting.count("SUBMIT_CARD"));
        assertEquals(1, reconnecting.count("RECONNECT_STATE"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reconnectCanStillPublishAnOldSnapshotAfterAnIndependentAction(boolean autoplay)
            throws Exception {
        TestSession reconnecting = newSession("reconnecting", true);
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
        assertEquals(10, snapshot.path("opponentCardCount").asInt());
        assertFalse(snapshot.path("floorCards").has("1"));
        assertFalse(snapshot.path("floorCards").has("7"));
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void busySnapshotDetachesSessionAndSameConnectionCanRetry(boolean replacing) throws Exception {
        TestSession current = replacing ? newSession("current", false) : null;
        if (current != null) current.emit(connectJson(USER_2));
        // 실제 WS 액션의 손패 저장을 지연해 재접속보다 먼저 게임 락을 획득한다.
        Sinks.Empty<Void> save = Sinks.empty();
        doAnswer(invocation -> {
            Mono<Void> actual = (Mono<Void>) invocation.callRealMethod();
            return save.asMono().then(actual);
        }).when(cards).updatePlayerCards(eq(ROOM_ID), eq(Player.PLAYER_1), anyList());
        submit();
        TestSession reconnecting = newSession("busy", false);
        reconnecting.emit(connectJson(USER_2));
        assertEquals("TRY_AGAIN", reconnecting.outbox().getLast().path("errorCode").asText());
        assertEquals(1, reconnecting.count("RECONNECT"));
        assertEquals(0, reconnecting.count("RECONNECT_STATE"));
        assertFalse(sessions.getPlayerContext(reconnecting.session()).hasElement().block(TIMEOUT));
        assertNull(sessions.getSession(ROOM_ID, 2));

        assertEquals(Sinks.EmitResult.OK, save.tryEmitEmpty());
        reconnecting.emit(connectJson(USER_2));
        assertEquals(1, reconnecting.count("RECONNECT_STATE"));
        assertEquals(2, reconnecting.data("RECONNECT_STATE").path("currentTurn").asInt());
        assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
        if (current != null) {
            await(() -> current.subscription().isDisposed());
            assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void completedActionDoesNotBroadcastAlreadySnapshottedCardsToANewSession(boolean replacing) throws Exception {
        TestSession previous = replacing ? newSession("before-action", false) : null;
        if (previous != null) previous.emit(connectJson(USER_2));
        Sinks.Empty<Void> sendCompleted = Sinks.empty();
        AtomicBoolean waitingForSend = new AtomicBoolean();
        // 세션 대역에서 SUBMIT_CARD 송신 완료만 보류한다. 상태 저장과 새 연결의 CONNECT는 지연하지 않는다.
        doAnswer(invocation -> Flux.from(
                        invocation.<Publisher<WebSocketMessage>>getArgument(0))
                .concatMap(message -> Mono.fromCallable(() -> mapper.readTree(message.getPayloadAsText())))
                .concatMap(node -> {
                    opponent.outbox().add(node);
                    if ("SUBMIT_CARD".equals(node.path("status").asText())) {
                        return sendCompleted.asMono().doOnSubscribe(ignored -> waitingForSend.set(true));
                    }
                    return Mono.empty();
                }).then()).when(opponent.session()).send(any());

        try {
            opponent.emit("{\"eventType\":{\"subType\":\"NORMAL_SUBMIT\"},\"data\":{\"cardIndex\":4}}");
            assertTrue(waitingForSend.get());
            assertEquals(2, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
            assertEquals(0, opponent.count("ACQUIRED_CARD"));

            TestSession reconnecting = newSession("after-save-before-notification", false);
            reconnecting.emit(connectJson(USER_2));
            JsonNode snapshot = reconnecting.data("RECONNECT_STATE");
            assertEquals(2, snapshot.path("currentTurn").asInt());
            assertEquals(9, snapshot.path("opponentCardCount").asInt());
            assertEquals(mapper.valueToTree(List.of(Card.MAY_1)),
                    snapshot.path("opponentAcquiredCards").path(Card.MAY_1.getType().name()));
            assertEquals(mapper.valueToTree(List.of(Card.MAY_2)),
                    snapshot.path("opponentAcquiredCards").path(Card.MAY_2.getType().name()));
            assertEquals(0, reconnecting.count("ACQUIRED_CARD"));
            assertEquals(0, reconnecting.count("SUBMIT_CARD"));

            assertEquals(Sinks.EmitResult.OK, sendCompleted.tryEmitEmpty());
            await(() -> opponent.count("ANNOUNCE_TURN_INFORMATION") == 1);
            assertEquals(1, opponent.count("ACQUIRED_CARD"), "기존 상대는 액션의 후속 안내를 모두 받는다");
            assertEquals(0, reconnecting.count("ACQUIRED_CARD"));
            assertEquals(0, reconnecting.count("SUBMIT_CARD"));
            assertEquals(0, reconnecting.count("CARD_REVEALED"));
            assertEquals(0, reconnecting.count("ANNOUNCE_TURN_INFORMATION"));
            if (previous != null) assertEquals(0, previous.count("ACQUIRED_CARD"));
            assertEquals(2, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());

            reconnecting.emit(connectJson(USER_2));
            assertEquals("ALREADY_JOIN", reconnecting.outbox().getLast().path("errorCode").asText());
            assertEquals(1, reconnecting.count("RECONNECT_STATE"));
            assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
        } finally {
            sendCompleted.tryEmitEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void snapshotErrorOrCancellationReleasesReadLockAndDetachesSession(boolean cancel) throws Exception {
        Sinks.One<List<Card>> read = Sinks.one();
        doReturn(read.asMono()).when(cards).getPlayerCards(ROOM_ID, Player.PLAYER_2);
        TestSession reconnecting = newSession("unfinished", false);
        reconnecting.emit(connectJson(USER_2));
        assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
        if (cancel) {
            reconnecting.subscription().dispose();
        } else {
            assertEquals(Sinks.EmitResult.OK, read.tryEmitError(new IllegalStateException("controlled read error")));
            assertEquals("SYSTEM_ERROR", reconnecting.outbox().getLast().path("errorCode").asText());
        }
        await(() -> sessions.getSession(ROOM_ID, 2) == null);
        assertFalse(sessions.getPlayerContext(reconnecting.session()).hasElement().block(TIMEOUT));
        doCallRealMethod().when(cards).getPlayerCards(ROOM_ID, Player.PLAYER_2);
        submit();
        await(() -> opponent.count("ANNOUNCE_TURN_INFORMATION") == 1);
        assertEquals(2, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
        assertSame(opponent.session(), sessions.getSession(ROOM_ID, 1));
        assertEquals(0, reconnecting.count("RECONNECT_STATE"));
    }

    @Test
    void cleanupWaitsForSnapshotReadAndDoesNotLeaveRegisteredSession() {
        List<Card> hand = cards.getPlayerCards(ROOM_ID, Player.PLAYER_2).block(TIMEOUT);
        Sinks.One<List<Card>> read = Sinks.one();
        doReturn(read.asMono()).when(cards).getPlayerCards(ROOM_ID, Player.PLAYER_2);
        TestSession reconnecting = newSession("cleanup-during-read", false);
        reconnecting.emit(connectJson(USER_2));
        AtomicBoolean cleaned = new AtomicBoolean();
        Disposable cleaning = cleanup.cleanupRoom(ROOM_ID).subscribe(ignored -> {},
                error -> fail(error), () -> cleaned.set(true));
        try {
            assertFalse(cleaned.get());
            assertNotNull(states.findById(ROOM_ID).block(TIMEOUT));
            assertEquals(Sinks.EmitResult.OK, read.tryEmitValue(hand));
            assertTrue(cleaned.get());
            assertNull(states.findById(ROOM_ID).block(TIMEOUT));
            assertTrue(sessions.getAllUser(ROOM_ID).isEmpty());
            assertFalse(sessions.getPlayerContext(reconnecting.session()).hasElement().block(TIMEOUT));
            assertEquals(0, reconnecting.count("RECONNECT_STATE"), "정리된 방의 스냅샷은 송신하지 않는다");
        } finally {
            cleaning.dispose();
        }
    }

    @Test
    void replacedConnectionDoesNotSendSnapshotWhileCloseIsPending() throws Exception {
        // 조회 사이의 선점 중 독립 CONNECT가 슬롯을 교체한다. 이전 소켓 close 완료는 지연될 수 있다.
        doAnswer(invocation -> {
            pauseHere();
            return invocation.callRealMethod();
        }).when(cards).getPlayerCards(ROOM_ID, Player.PLAYER_2);
        TestSession old = newSession("replaced-during-read", false);
        Sinks.Empty<Void> closed = Sinks.empty();
        doReturn(closed.asMono()).when(old.session()).close();
        var connecting = reconnectWorker.submit(() -> old.emit(connectJson(USER_2)));
        try {
            assertTrue(paused.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            TestSession replacement = newSession("replacement-during-read", false);
            replacement.emit(connectJson(USER_2));
            assertEquals("TRY_AGAIN", replacement.outbox().getLast().path("errorCode").asText());
            assertNull(sessions.getSession(ROOM_ID, 2));
            verify(old.session()).close();
            assertFalse(old.subscription().isDisposed());

            resume.countDown();
            connecting.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertEquals(0, old.count("RECONNECT_STATE"));
            replacement.emit(connectJson(USER_2));
            assertEquals(1, replacement.count("RECONNECT_STATE"));
            assertSame(replacement.session(), sessions.getSession(ROOM_ID, 2));

            old.drop();
            assertSame(replacement.session(), sessions.getSession(ROOM_ID, 2));
            assertNotNull(states.findById(ROOM_ID).block(TIMEOUT));
        } finally {
            resume.countDown();
            closed.tryEmitEmpty();
        }
    }

    private void submit() {
        opponent.emit("{\"eventType\":{\"subType\":\"NORMAL_SUBMIT\"},\"data\":{\"cardIndex\":0}}");
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
