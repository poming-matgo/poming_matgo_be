package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.application.room.RoomService;
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

// 실제 액션·CONNECT 경로와 제어 송신으로 조회 경계 및 스냅샷 이후 안내 순서를 검증한다.
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
    @Autowired RoomService rooms;
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
    void independentActionWaitsForSnapshotDeliveryOutsideGameLock(boolean autoplay)
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
        await(() -> opponent.count("SUBMIT_CARD") == 1);
        assertEquals(0, reconnecting.count("ANNOUNCE_TURN_INFORMATION"));
        assertEquals(0, reconnecting.count("SUBMIT_CARD"));
        assertEquals(0, reconnecting.count("RECONNECT_STATE"));
        assertEquals(2, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
        assertEquals(9, cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).size());
        assertEquals(10, cards.getAllRevealedCards(ROOM_ID).block(TIMEOUT).size());

        resume.countDown();
        connect.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        await(() -> reconnecting.count("ANNOUNCE_TURN_INFORMATION") == 1);
        JsonNode snapshot = reconnecting.data("RECONNECT_STATE");
        assertEquals(1, snapshot.path("currentTurn").asInt());
        assertEquals("PLAYER_1", snapshot.path("currentPlayer").asText());
        assertEquals(10, snapshot.path("myCards").size());
        assertEquals(10, snapshot.path("opponentCardCount").asInt());
        assertFalse(snapshot.path("floorCards").has("1"));
        assertFalse(snapshot.path("floorCards").has("7"));
        assertEquals("PLAYER_2", reconnecting.data("ANNOUNCE_TURN_INFORMATION").path("curPlayer").asText());
        assertEquals(2, reconnecting.data("ANNOUNCE_TURN_INFORMATION").path("turn").asInt());
        List<String> order = reconnecting.outbox().stream().map(node -> node.path("status").asText()).toList();
        assertTrue(order.indexOf("RECONNECT_STATE") < order.indexOf("SUBMIT_CARD"));
        assertTrue(order.indexOf("RECONNECT_STATE") < order.indexOf("ANNOUNCE_TURN_INFORMATION"));
        assertEquals(1, reconnecting.count("SUBMIT_CARD"));
        assertEquals(1, reconnecting.count("CARD_REVEALED"));
        assertEquals(1, reconnecting.count("RECONNECT_STATE"));
        assertTrue(reconnecting.outbox().stream().noneMatch(node -> node.has("errorCode")));
        assertSame(opponent.session(), sessions.getSession(ROOM_ID, 1));
        assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
        assertFalse(reconnecting.subscription().isDisposed());
    }

    @Test
    void actionBeforeSnapshotReadIsIncludedOnlyInSnapshot() throws Exception {
        TestSession reconnecting = newSession("registered-before-read", false);
        Sinks.Empty<Void> reconnectNotice = Sinks.empty();
        doAnswer(invocation -> Flux.from(invocation.<Publisher<WebSocketMessage>>getArgument(0))
                .concatMap(message -> Mono.fromCallable(() -> mapper.readTree(message.getPayloadAsText())))
                .concatMap(node -> {
                    reconnecting.outbox().add(node);
                    return "RECONNECT".equals(node.path("status").asText())
                            ? reconnectNotice.asMono() : Mono.empty();
                }).then()).when(reconnecting.session()).send(any());
        reconnecting.emit(connectJson(USER_2));
        assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
        assertEquals(0, reconnecting.count("RECONNECT_STATE"));
        opponent.emit("{\"eventType\":{\"subType\":\"NORMAL_SUBMIT\"},\"data\":{\"cardIndex\":4}}");
        await(() -> opponent.count("ANNOUNCE_TURN_INFORMATION") == 1);
        assertEquals(0, reconnecting.count("SUBMIT_CARD"));
        assertEquals(0, reconnecting.count("ACQUIRED_CARD"));
        assertEquals(Sinks.EmitResult.OK, reconnectNotice.tryEmitEmpty());
        assertEquals(2, reconnecting.data("RECONNECT_STATE").path("currentTurn").asInt());
        assertEquals(mapper.valueToTree(List.of(Card.MAY_1)),
                reconnecting.data("RECONNECT_STATE").path("opponentAcquiredCards").path(Card.MAY_1.getType().name()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unfinishedSnapshotSendReleasesWaitingActionOnFailureOrDisconnect(boolean cancel) throws Exception {
        TestSession reconnecting = newSession("unfinished-send", false);
        Sinks.Empty<Void> snapshotSend = Sinks.empty();
        doAnswer(invocation -> Flux.from(invocation.<Publisher<WebSocketMessage>>getArgument(0))
                .concatMap(message -> Mono.fromCallable(() -> mapper.readTree(message.getPayloadAsText())))
                .concatMap(node -> {
                    if ("RECONNECT_STATE".equals(node.path("status").asText())) return snapshotSend.asMono();
                    reconnecting.outbox().add(node);
                    return Mono.empty();
                }).then()).when(reconnecting.session()).send(any());
        reconnecting.emit(connectJson(USER_2));
        submit();
        assertEquals(2, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
        assertEquals(0, reconnecting.count("SUBMIT_CARD"));
        if (cancel) reconnecting.subscription().dispose();
        else assertEquals(Sinks.EmitResult.OK, snapshotSend.tryEmitError(new IllegalStateException("send failed")));
        await(() -> sessions.getSession(ROOM_ID, 2) == null);
        await(() -> opponent.count("ANNOUNCE_TURN_INFORMATION") == 1);
        assertEquals(0, reconnecting.count("SUBMIT_CARD"));
        assertEquals(0, reconnecting.count("ANNOUNCE_TURN_INFORMATION"));
        if (!cancel) {
            assertEquals("SYSTEM_ERROR", reconnecting.outbox().getLast().path("errorCode").asText());
            // 동일 소켓 재등록은 새 조회 경계를 사용한다.
            doAnswer(invocation -> Flux.from(invocation.<Publisher<WebSocketMessage>>getArgument(0))
                    .doOnNext(message -> {
                        try { reconnecting.outbox().add(mapper.readTree(message.getPayloadAsText())); }
                        catch (Exception error) { throw new IllegalStateException(error); }
                    }).then()).when(reconnecting.session()).send(any());
            reconnecting.emit(connectJson(USER_2));
            assertEquals(2, reconnecting.data("RECONNECT_STATE").path("currentTurn").asInt());
        }
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nextAutoplayWaitsForPreviousActionsEntireNotification(boolean delayFirstSend) throws Exception {
        TestSession reconnecting = newSession("multiple-actions", false);
        reconnecting.emit(connectJson(USER_2));
        assertEquals(1, reconnecting.data("RECONNECT_STATE").path("currentTurn").asInt());
        Sinks.Empty<Void> firstSend = Sinks.empty();
        AtomicBoolean intercepted = new AtomicBoolean();
        // 첫 사용자 액션의 송신만 지연한다. 다음 플레이어의 타이머·락·InFlight는 실제 경로를 쓴다.
        doAnswer(invocation -> Flux.from(invocation.<Publisher<WebSocketMessage>>getArgument(0))
                .concatMap(message -> Mono.fromCallable(() -> mapper.readTree(message.getPayloadAsText())))
                .concatMap(node -> {
                    opponent.outbox().add(node);
                    if (delayFirstSend && "SUBMIT_CARD".equals(node.path("status").asText())
                            && intercepted.compareAndSet(false, true)) return firstSend.asMono();
                    return Mono.empty();
                }).then()).when(opponent.session()).send(any());
        try {
            submit();
            GameState next = states.findById(ROOM_ID).block(TIMEOUT);
            assertEquals(2, next.getCurrentTurn());
            assertEquals(GamePhase.IN_PROGRESS, next.getPhase());
            assertEquals(Player.PLAYER_2, next.getCurrentPlayer());
            verify(autoPlay).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(2), eq(Player.PLAYER_2),
                    anyLong(), eq(GamePhase.IN_PROGRESS));
            assertEquals(delayFirstSend ? 1 : 0, firstSend.currentSubscriberCount());
            assertEquals(delayFirstSend ? 0 : 1, reconnecting.count("ANNOUNCE_TURN_INFORMATION"));

            // 정상 등록된 다음 턴의 기한만 앞당긴다. 상태·phase·실행 플래그는 변경하지 않는다.
            autoPlay.scheduleAutoPlay(ROOM_ID, next.getRound(), next.getCurrentTurn(),
                    next.getCurrentPlayer(), System.nanoTime(), next.getPhase());
            await(() -> states.findById(ROOM_ID).block(TIMEOUT).getRound() == 2);
            assertEquals(1, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
            assertEquals(2, states.findById(ROOM_ID).block(TIMEOUT).getRound());
            assertEquals(9, cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).size());
            assertEquals(9, cards.getPlayerCards(ROOM_ID, Player.PLAYER_2).block(TIMEOUT).size());
            if (delayFirstSend) {
                assertEquals(1, reconnecting.count("SUBMIT_CARD"));
                assertEquals(0, reconnecting.count("SCORE_UPDATE"));
                assertEquals(0, reconnecting.count("ANNOUNCE_TURN_INFORMATION"));
            }
            assertEquals(Sinks.EmitResult.OK, firstSend.tryEmitEmpty());
            await(() -> reconnecting.count("ANNOUNCE_TURN_INFORMATION") == 2);

            List<String> expected = List.of("1:2", "2:1");
            for (TestSession client : List.of(opponent, reconnecting)) {
                assertEquals(expected, client.outbox().stream()
                        .filter(node -> "ANNOUNCE_TURN_INFORMATION".equals(node.path("status").asText()))
                        .map(node -> node.path("data").path("round").asInt() + ":"
                                + node.path("data").path("turn").asInt()).toList());
                assertEquals(2, client.count("SUBMIT_CARD"));
                List<String> statuses = client.outbox().stream().map(node -> node.path("status").asText()).toList();
                assertTrue(statuses.indexOf("SCORE_UPDATE") < statuses.indexOf("ANNOUNCE_TURN_INFORMATION"));
                assertTrue(statuses.indexOf("ANNOUNCE_TURN_INFORMATION") < statuses.lastIndexOf("SUBMIT_CARD"));
                assertTrue(client.outbox().stream().noneMatch(node -> node.has("errorCode")));
            }
            assertSame(opponent.session(), sessions.getSession(ROOM_ID, 1));
            assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
        } finally {
            firstSend.tryEmitEmpty();
        }
    }

    @Test
    void sessionPreservingRestartCompletesWhileGameOverWaitsForSnapshot() throws Exception {
        TestSession playing = newSession("playing-before-restart", false);
        playing.emit(connectJson(USER_2));
        GameState state = playUntilThirdPpeokIsNext(playing);
        playing.drop();
        await(() -> sessions.getSession(ROOM_ID, 2) == null);

        TestSession reconnecting = newSession("snapshot-across-restart", true);
        var connect = reconnectWorker.submit(() -> reconnecting.emit(connectJson(USER_2)));
        try {
            assertTrue(paused.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            autoPlay.scheduleAutoPlay(ROOM_ID, state.getRound(), state.getCurrentTurn(),
                    state.getCurrentPlayer(), System.nanoTime(), state.getPhase());
            await(() -> {
                GameState recreated = states.findById(ROOM_ID).block(TIMEOUT);
                return recreated != null && recreated.getPhase() == GamePhase.NONE;
            });
            assertEquals(GamePhase.NONE, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
            assertTrue(cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).isEmpty());
            assertTrue(cards.getPlayerCards(ROOM_ID, Player.PLAYER_2).block(TIMEOUT).isEmpty());
            assertSame(opponent.session(), sessions.getSession(ROOM_ID, 1));
            assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
            assertEquals(0, reconnecting.count("RECONNECT_STATE"));
            assertEquals(0, reconnecting.count("GAME_OVER"));

            resume.countDown();
            connect.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            await(() -> reconnecting.count("GAME_OVER") == 1);
            assertEquals("IN_PROGRESS", reconnecting.data("RECONNECT_STATE").path("phase").asText());
            assertEquals(state.getRound(), reconnecting.data("RECONNECT_STATE").path("round").asInt());
            assertEquals(state.getCurrentTurn(), reconnecting.data("RECONNECT_STATE").path("currentTurn").asInt());
            assertEquals(1, opponent.count("GAME_OVER"));
            assertEquals(1, reconnecting.count("THREE_PPEOK"));
            assertEquals(state.getCurrentPlayer().name(), reconnecting.data("GAME_OVER").path("winner").asText());
            List<String> order = reconnecting.outbox().stream().map(node -> node.path("status").asText()).toList();
            assertTrue(order.indexOf("RECONNECT_STATE") < order.indexOf("SUBMIT_CARD"));
            assertTrue(order.indexOf("RECONNECT_STATE") < order.indexOf("GAME_OVER"));
            assertTrue(reconnecting.outbox().stream().noneMatch(node -> node.has("errorCode")));
            assertSame(reconnecting.session(), sessions.getSession(ROOM_ID, 2));
        } finally {
            resume.countDown();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void baselineReadyCanOvertakePreviousGameOverButStartWaitsForBothPlayers(boolean delayGameOver)
            throws Exception {
        TestSession playing = newSession("ready-after-game-over", false);
        playing.emit(connectJson(USER_2));
        GameState finalTurn = playUntilThirdPpeokIsNext(playing);
        Sinks.Empty<Void> gameOverSend = Sinks.empty();
        // 자동플레이 송신과 양쪽 WS 수신은 독립적이다. 두 클라이언트 모두 자신의 종료 안내 후 재준비한다.
        doAnswer(invocation -> Flux.from(invocation.<Publisher<WebSocketMessage>>getArgument(0))
                .concatMap(message -> Mono.fromCallable(() -> mapper.readTree(message.getPayloadAsText())))
                .concatMap(node -> {
                    Mono<Void> wait = delayGameOver && "GAME_OVER".equals(node.path("status").asText())
                            ? gameOverSend.asMono() : Mono.empty();
                    return wait.then(Mono.fromRunnable(() -> playing.outbox().add(node)));
                }).then()).when(playing.session()).send(any());
        try {
            autoPlay.scheduleAutoPlay(ROOM_ID, finalTurn.getRound(), finalTurn.getCurrentTurn(),
                    finalTurn.getCurrentPlayer(), System.nanoTime(), finalTurn.getPhase());
            await(() -> opponent.count("GAME_OVER") == 1
                    && (delayGameOver ? gameOverSend.currentSubscriberCount() == 1 : playing.count("GAME_OVER") == 1));
            assertEquals(GamePhase.NONE, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
            assertTrue(cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).isEmpty());
            assertTrue(cards.getPlayerCards(ROOM_ID, Player.PLAYER_2).block(TIMEOUT).isEmpty());

            // REST가 사용하는 프록시를 통해 같은 슬롯 순서로 재참여한다. 저장소·phase를 직접 변경하지 않는다.
            rooms.joinRoom(USER_1, ROOM_ID).block(TIMEOUT);
            opponent.emit("{\"eventType\":{\"subType\":\"READY\"}}");
            await(() -> playing.count("READY") == 1);
            GameState oneReady = states.findById(ROOM_ID).block(TIMEOUT);
            assertEquals(GamePhase.NONE, oneReady.getPhase());
            assertTrue(oneReady.getPlayerState(Player.PLAYER_1).isReady());
            assertFalse(oneReady.getPlayerState(Player.PLAYER_2).isReady());
            assertEquals(delayGameOver ? 0 : 1, playing.count("GAME_OVER"));
            assertEquals(0, playing.count("START"));
            assertEquals(0, opponent.count("START"));

            assertEquals(Sinks.EmitResult.OK, gameOverSend.tryEmitEmpty());
            await(() -> playing.count("GAME_OVER") == 1);
            rooms.joinRoom(USER_2, ROOM_ID).block(TIMEOUT);
            playing.emit("{\"eventType\":{\"subType\":\"READY\"}}");
            await(() -> playing.count("START") == 1 && opponent.count("START") == 1);
            assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
            assertEquals(List.of(delayGameOver ? "READY" : "GAME_OVER", delayGameOver ? "GAME_OVER" : "READY",
                            "READY", "START"),
                    playing.outbox().stream().map(node -> node.path("status").asText())
                            .filter(status -> List.of("GAME_OVER", "READY", "START").contains(status)).toList());
            for (TestSession client : List.of(opponent, playing)) {
                assertEquals(1, client.count("GAME_OVER"));
                assertEquals(2, client.count("READY"));
                assertTrue(client.outbox().stream().noneMatch(node -> node.has("errorCode")));
            }
            assertSame(opponent.session(), sessions.getSession(ROOM_ID, 1));
            assertSame(playing.session(), sessions.getSession(ROOM_ID, 2));
        } finally {
            gameOverSend.tryEmitEmpty();
        }
    }

    private GameState playUntilThirdPpeokIsNext(TestSession playing) throws Exception {
        // 종료 직전 상태를 주입하지 않고 고정 덱의 실제 WS 제출로 세 번째 뻑 직전에 도달한다.
        GameState state = states.findById(ROOM_ID).block(TIMEOUT);
        for (int actions = 0; actions < 60
                && state.getPlayerState(state.getCurrentPlayer()).getPpeokCount() < 2; actions++) {
            assertTrue(state.getPhase().isPlayerActionPhase());
            TestSession actor = state.getCurrentPlayer() == Player.PLAYER_1 ? opponent : playing;
            String action = state.getPhase() == GamePhase.AWAITING_FLOOR_CARD_CHOICE
                    ? "FLOOR_SELECT" : "NORMAL_SUBMIT";
            actor.emit("{\"eventType\":{\"subType\":\"" + action + "\"},\"data\":{\"cardIndex\":0}}");
            GameState before = state;
            await(() -> states.findById(ROOM_ID).block(TIMEOUT) != before);
            state = states.findById(ROOM_ID).block(TIMEOUT);
            assertTrue(actor.outbox().stream().noneMatch(node -> node.has("errorCode")));
        }
        assertEquals(2, state.getPlayerState(state.getCurrentPlayer()).getPpeokCount());
        assertEquals(GamePhase.IN_PROGRESS, state.getPhase());
        return state;
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
