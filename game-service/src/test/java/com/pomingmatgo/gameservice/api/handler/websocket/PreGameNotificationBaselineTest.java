package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.application.pregame.PreGameService;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.application.room.RoomService;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryInstalledCardRepository;
import com.pomingmatgo.gameservice.infrastructure.scheduler.AutoPlayScheduler;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// 실제 WS 준비·선택과 자동플레이의 제어 송신 순서 검증이다. 실제 TCP 수신·적용 보장은 별도다.
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@ActiveProfiles("in-memory")
class PreGameNotificationBaselineTest {
    private static final long ROOM_ID = 960_087L;
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
    @Autowired InMemoryInstalledCardRepository cards;
    @Autowired LeadingPlayerRepository leaders;
    @SpyBean PreGameService preGame;

    private final List<TestSession> clients = new ArrayList<>();
    private TestSession first;
    private TestSession second;
    private final Sinks.Empty<Void> releaseSend = Sinks.empty();

    @BeforeEach
    void setUp() throws Exception {
        // 무작위 입력만 고정한다. READY 이후 선택·분배·phase 전이는 실제 서비스가 수행한다.
        doAnswer(invocation -> leaders.saveSelectedCard(
                List.of(Card.DEC_1, Card.JAN_1, Card.FEB_1, Card.MAR_1, Card.APR_1), ROOM_ID))
                .when(preGame).pickFiveCardsAndSave(ROOM_ID);
        List<Card> hand1 = List.of(Card.JAN_1, Card.FEB_1, Card.MAR_1, Card.APR_1, Card.MAY_2,
                Card.JUN_2, Card.JUL_2, Card.AUG_1, Card.SEP_1, Card.OCT_1);
        List<Card> hand2 = List.of(Card.JAN_2, Card.FEB_2, Card.MAR_2, Card.APR_2, Card.MAY_3,
                Card.JUN_3, Card.JUL_3, Card.AUG_2, Card.SEP_2, Card.NOV_1);
        List<Card> floor = List.of(Card.MAY_1, Card.JUN_1, Card.AUG_3, Card.SEP_3,
                Card.OCT_2, Card.NOV_2, Card.DEC_1, Card.DEC_2);
        List<Card> deck = new ArrayList<>(hand1);
        deck.addAll(hand2);
        deck.addAll(floor);
        deck.add(Card.JUL_1);
        Arrays.stream(Card.values()).filter(card -> !deck.contains(card)).forEach(deck::add);
        assertEquals(48, deck.size());
        doAnswer(invocation -> preGame.distributeCards(ROOM_ID, deck)).when(preGame).distributeCards(ROOM_ID);

        states.create(GameState.createEmptyRoom(ROOM_ID)).block(TIMEOUT);
        rooms.joinRoom(USER_1, ROOM_ID).block(TIMEOUT);
        rooms.joinRoom(USER_2, ROOM_ID).block(TIMEOUT);
        first = newSession("first");
        second = newSession("second");
        first.emit(connectJson(USER_1));
        second.emit(connectJson(USER_2));
        first.emit("{\"eventType\":{\"subType\":\"READY\"}}");
        second.emit("{\"eventType\":{\"subType\":\"READY\"}}");
        await(() -> first.count("START") == 1 && second.count("START") == 1);
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
    }

    @AfterEach
    void tearDown() {
        releaseSend.tryEmitEmpty();
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
        clients.forEach(client -> client.subscription().dispose());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void secondSelectionWaitsForFirstSelectionDelivery(boolean delayed) throws Exception {
        holdSend(first, "LEADER_SELECTION", delayed);
        select(first, 0);
        if (delayed) assertEquals(1, releaseSend.currentSubscriberCount());
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        assertEquals(0, first.count("LEADER_SELECTION_RESULT"));

        // 양쪽 START 이후 독립 연결의 선택이다. 첫 연결의 수신 concatMap을 우회하지 않는다.
        select(second, 1);
        if (delayed) assertEquals(0, first.count("LEADER_SELECTION_RESULT"));
        assertEquals(GamePhase.IN_PROGRESS, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        assertEquals(delayed ? 0 : 2, first.count("LEADER_SELECTION"));
        assertEquals(10, cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).size());
        releaseSend.tryEmitEmpty();
        await(() -> first.count("LEADER_SELECTION_RESULT") == 1);
        assertEquals(List.of("PLAYER_1", "PLAYER_2", "RESULT"),
                first.outbox().stream().filter(node -> List.of("LEADER_SELECTION", "LEADER_SELECTION_RESULT")
                                .contains(node.path("status").asText()))
                        .map(node -> "LEADER_SELECTION_RESULT".equals(node.path("status").asText())
                                ? "RESULT" : node.path("player").asText()).toList());
        assertHealthy();
    }

    @ParameterizedTest
    @CsvSource({"DISTRIBUTE_CARD,false", "DISTRIBUTE_CARD,true",
            "ANNOUNCE_TURN_INFORMATION,false", "ANNOUNCE_TURN_INFORMATION,true"})
    void firstAutoplayProgressesButNotificationsWaitForStart(String heldStatus, boolean delayed) throws Exception {
        select(first, 0);
        holdSend(first, heldStatus, delayed);
        select(second, 1);
        if (delayed) assertEquals(1, releaseSend.currentSubscriberCount());
        GameState initial = states.findById(ROOM_ID).block(TIMEOUT);
        assertEquals(GamePhase.IN_PROGRESS, initial.getPhase());
        assertEquals(Player.PLAYER_1, initial.getCurrentPlayer());
        assertEquals(1, initial.getCurrentTurn());
        verify(autoPlay).scheduleAutoPlay(eq(ROOM_ID), eq(1), eq(1), eq(Player.PLAYER_1),
                anyLong(), eq(GamePhase.IN_PROGRESS));

        // 실제 등록된 첫 턴 기한만 앞당긴다. 선택 송신 주체(PLAYER_2)와 자동플레이 주체는 다르다.
        autoPlay.scheduleAutoPlay(ROOM_ID, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.IN_PROGRESS);
        await(() -> states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn() == 2);
        if (delayed) {
            assertEquals(0, first.count("SUBMIT_CARD"));
            assertEquals(0, first.count("ANNOUNCE_TURN_INFORMATION"));
        }
        assertEquals(2, states.findById(ROOM_ID).block(TIMEOUT).getCurrentTurn());
        assertEquals(9, cards.getPlayerCards(ROOM_ID, Player.PLAYER_1).block(TIMEOUT).size());
        if (delayed && "DISTRIBUTE_CARD".equals(heldStatus)) assertEquals(0, first.count("DISTRIBUTE_CARD"));
        releaseSend.tryEmitEmpty();
        await(() -> first.count("ANNOUNCE_TURN_INFORMATION") == 2);
        assertEquals(List.of(1, 2), first.outbox().stream()
                .filter(node -> "ANNOUNCE_TURN_INFORMATION".equals(node.path("status").asText()))
                .map(node -> node.path("data").path("turn").asInt()).toList());
        List<String> statuses = first.outbox().stream().map(node -> node.path("status").asText()).toList();
        assertTrue(statuses.indexOf("DISTRIBUTE_CARD") < statuses.indexOf("SUBMIT_CARD"));
        assertHealthy();
    }

    @Test
    void reconnectDuringStartDeliveryReceivesSnapshotWithoutOldStartNotifications() throws Exception {
        select(first, 0);
        holdSend(first, "DISTRIBUTE_CARD", true);
        select(second, 1);
        assertEquals(1, releaseSend.currentSubscriberCount());
        assertEquals(GamePhase.IN_PROGRESS, states.findById(ROOM_ID).block(TIMEOUT).getPhase());

        TestSession replacement = newSession("replacement");
        replacement.emit(connectJson(USER_1));
        await(() -> replacement.count("RECONNECT_STATE") == 1);
        releaseSend.tryEmitEmpty();
        await(() -> second.count("ANNOUNCE_TURN_INFORMATION") == 1);

        assertSame(replacement.session(), sessions.getSession(ROOM_ID, 1));
        assertEquals(0, replacement.count("LEADER_SELECTION"));
        assertEquals(0, replacement.count("LEADER_SELECTION_RESULT"));
        assertEquals(0, replacement.count("DISTRIBUTE_CARD"));
        assertEquals(0, replacement.count("ANNOUNCE_TURN_INFORMATION"));
        assertTrue(replacement.outbox().stream().noneMatch(node -> node.has("errorCode")));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void disconnectCleanupDoesNotWaitForAlreadyStartedSelectionSendBaseline(boolean delayed) throws Exception {
        holdSend(first, "LEADER_SELECTION", delayed);
        select(first, 0);
        assertEquals(delayed ? 1 : 0, releaseSend.currentSubscriberCount());
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());

        // 상대 연결의 정상 종료는 첫 연결의 선택 송신과 독립적이다.
        assertEquals(Sinks.EmitResult.OK, second.inbound().tryEmitComplete());
        await(() -> first.count("OPPONENT_DISCONNECTED") == 1 && sessions.getAllUser(ROOM_ID).isEmpty());
        assertNull(states.findById(ROOM_ID).block(TIMEOUT));
        assertFalse(sessions.getPlayerContext(first.session()).hasElement().block(TIMEOUT));
        assertFalse(first.subscription().isDisposed());
        assertEquals(delayed ? 0 : 1, first.count("LEADER_SELECTION"));
        // 현재 정리는 매핑·예약을 제거하지만 이미 시작한 호출자 소유 송신은 취소하지 않는다.
        assertEquals(delayed ? 1 : 0, releaseSend.currentSubscriberCount());
        releaseSend.tryEmitEmpty();
        await(() -> first.count("LEADER_SELECTION") == 1);
        assertEquals(delayed
                        ? List.of("OPPONENT_DISCONNECTED", "LEADER_SELECTION")
                        : List.of("LEADER_SELECTION", "OPPONENT_DISCONNECTED"),
                first.outbox().stream().map(node -> node.path("status").asText())
                        .filter(status -> List.of("LEADER_SELECTION", "OPPONENT_DISCONNECTED").contains(status))
                        .toList());
        assertEquals(0, first.count("LEADER_SELECTION_RESULT"));
        assertEquals(0, first.count("DISTRIBUTE_CARD"));
        assertEquals(0, releaseSend.currentSubscriberCount());
        assertTrue(first.outbox().stream().noneMatch(node -> node.has("errorCode")));
    }

    @Test
    void cleanupLeavesSocketOpenButRequiresRoomAndMembershipBeforeConnect() throws Exception {
        assertEquals(Sinks.EmitResult.OK, second.inbound().tryEmitComplete());
        await(() -> sessions.getAllUser(ROOM_ID).isEmpty());
        assertNull(states.findById(ROOM_ID).block(TIMEOUT));

        first.emit("{\"eventType\":{\"subType\":\"READY\"}}");
        first.emit(connectJson(USER_1));
        await(() -> errors(first).size() == 2);
        assertEquals(List.of("NOT_IN_ROOM", "NOT_EXISTED_ROOM"), errors(first));

        rooms.createRoom(ROOM_ID).block(TIMEOUT);
        first.emit(connectJson(USER_1));
        await(() -> errors(first).size() == 3);
        assertEquals("NOT_IN_ROOM", errors(first).get(2));
        assertFalse(sessions.getPlayerContext(first.session()).hasElement().block(TIMEOUT));

        rooms.joinRoom(USER_1, ROOM_ID).block(TIMEOUT);
        long connects = first.count("CONNECT");
        first.emit(connectJson(USER_1));
        await(() -> first.count("CONNECT") == connects + 1);
        assertSame(first.session(), sessions.getSession(ROOM_ID, 1));
        assertFalse(first.subscription().isDisposed());
        verify(first.session(), never()).close();
        assertEquals(0, first.count("RECONNECT_STATE"));
        assertEquals(GamePhase.NONE, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sameSocketCanRejoinRecreatedRoomAfterEarlierSelectionSendFinishes(boolean delayed) throws Exception {
        holdSend(first, "LEADER_SELECTION", delayed);
        select(first, 0);
        assertEquals(delayed ? 1 : 0, releaseSend.currentSubscriberCount());
        assertEquals(Sinks.EmitResult.OK, second.inbound().tryEmitComplete());
        await(() -> first.count("OPPONENT_DISCONNECTED") == 1 && sessions.getAllUser(ROOM_ID).isEmpty());
        assertNull(states.findById(ROOM_ID).block(TIMEOUT));

        rooms.createRoom(ROOM_ID).block(TIMEOUT);
        rooms.joinRoom(USER_1, ROOM_ID).block(TIMEOUT);
        rooms.joinRoom(USER_2, ROOM_ID).block(TIMEOUT);
        long connects = first.count("CONNECT");
        // 기존 핸들러에 요청을 넣어 concatMap 순서를 유지한다. 새 방 생성·참여만 독립 HTTP 서비스 경로다.
        first.emit(connectJson(USER_1));
        if (delayed) {
            assertEquals(connects, first.count("CONNECT"));
            assertNull(sessions.getSession(ROOM_ID, 1));
            assertEquals(1, releaseSend.currentSubscriberCount());
        }
        releaseSend.tryEmitEmpty();
        await(() -> first.count("CONNECT") == connects + 1);
        assertSame(first.session(), sessions.getSession(ROOM_ID, 1));
        assertFalse(first.subscription().isDisposed());
        verify(first.session(), never()).close();
        assertEquals(0, first.count("RECONNECT_STATE"));

        List<String> statuses = first.outbox().stream().map(node -> node.path("status").asText()).toList();
        assertTrue(statuses.lastIndexOf("LEADER_SELECTION") < statuses.lastIndexOf("CONNECT"));

        TestSession peer = newSession("new-peer");
        peer.emit(connectJson(USER_2));
        await(() -> peer.count("CONNECT") == 1);
        assertEquals(0, peer.count("LEADER_SELECTION"));
        assertEquals(0, peer.count("LEADER_SELECTION_RESULT"));
        first.emit("{\"eventType\":{\"subType\":\"READY\"}}");
        peer.emit("{\"eventType\":{\"subType\":\"READY\"}}");
        await(() -> first.count("START") == 2 && peer.count("START") == 1);
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, states.findById(ROOM_ID).block(TIMEOUT).getPhase());
        assertEquals(0, first.count("LEADER_SELECTION_RESULT"));
        assertEquals(0, first.count("DISTRIBUTE_CARD"));
        assertTrue(errors(first).isEmpty());
        assertTrue(errors(peer).isEmpty());
    }

    private static List<String> errors(TestSession client) {
        return client.outbox().stream().filter(node -> node.has("errorCode"))
                .map(node -> node.path("errorCode").asText()).toList();
    }

    private void select(TestSession client, int index) {
        client.emit("{\"eventType\":{\"subType\":\"LEADER_SELECTION\"},\"data\":{\"cardIndex\":" + index + "}}");
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

    private void assertHealthy() {
        for (TestSession client : clients) {
            assertTrue(client.outbox().stream().noneMatch(node -> node.has("errorCode")));
            assertEquals(1, client.count("LEADER_SELECTION_RESULT"));
            assertEquals(1, client.count("DISTRIBUTE_CARD"));
        }
        assertSame(first.session(), sessions.getSession(ROOM_ID, 1));
        assertSame(second.session(), sessions.getSession(ROOM_ID, 2));
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
