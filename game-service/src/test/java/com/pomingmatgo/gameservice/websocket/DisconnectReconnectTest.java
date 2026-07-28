package com.pomingmatgo.gameservice.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.api.handler.websocket.GameWebSocketHandler;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import com.pomingmatgo.gameservice.scheduler.AutoPlayScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@DisplayName("이탈/재접속 처리 통합 테스트")
class DisconnectReconnectTest {

    private static final long USER_1 = 101L;
    private static final long USER_2 = 102L;

    @Autowired GameWebSocketHandler handler;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired InstalledCardRepository installedCardRepository;
    @Autowired SessionManager sessionManager;
    @Autowired RoomCleanupService roomCleanupService;
    @Autowired AutoPlayScheduler autoPlayScheduler;
    @Autowired ObjectMapper objectMapper;

    private long roomId;

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(roomId).block();
        sessionManager.removeRoom(roomId).block();
    }

    /** 인바운드/아웃바운드를 조작·관측할 수 있는 WebSocketSession 목 */
    private record TestSession(WebSocketSession session,
                               Sinks.Many<WebSocketMessage> inbound,
                               List<String> outbox,
                               AtomicBoolean closed) {

        void emit(String json) {
            WebSocketMessage msg = Mockito.mock(WebSocketMessage.class);
            when(msg.getPayloadAsText()).thenReturn(json);
            inbound.tryEmitNext(msg);
        }

        /** 클라이언트 측 연결 끊김 시뮬레이션 (receive 스트림 종료 → doFinally disconnect 처리) */
        void drop() {
            inbound.tryEmitComplete();
        }

        boolean received(String status) {
            return outbox.stream().anyMatch(m -> m.contains("\"" + status + "\""));
        }
    }

    @SuppressWarnings("unchecked")
    private TestSession newSession(String id) {
        Sinks.Many<WebSocketMessage> inbound = Sinks.many().unicast().onBackpressureBuffer();
        List<String> outbox = new CopyOnWriteArrayList<>();
        AtomicBoolean closed = new AtomicBoolean(false);

        WebSocketSession s = Mockito.mock(WebSocketSession.class);
        when(s.getId()).thenReturn(id);
        when(s.receive()).thenReturn(inbound.asFlux());
        when(s.isOpen()).thenAnswer(inv -> !closed.get());
        when(s.textMessage(anyString())).thenAnswer(inv -> {
            WebSocketMessage m = Mockito.mock(WebSocketMessage.class);
            when(m.getPayloadAsText()).thenReturn(inv.getArgument(0));
            return m;
        });
        when(s.send(any())).thenAnswer(inv ->
                Flux.from((Publisher<WebSocketMessage>) inv.getArgument(0))
                        .doOnNext(m -> outbox.add(m.getPayloadAsText()))
                        .then());
        // 서버 측 kick: 스트림 종료로 이어져 disconnect 흐름까지 동일하게 탄다
        when(s.close()).thenReturn(Mono.fromRunnable(() -> {
            closed.set(true);
            inbound.tryEmitComplete();
        }));

        TestSession ts = new TestSession(s, inbound, outbox, closed);
        handler.handle(s).subscribe();
        return ts;
    }

    private String connectJson(long userId) {
        return "{\"eventType\":{\"type\":\"JOIN_ROOM\",\"subType\":\"CONNECT\"},"
                + "\"data\":{\"userId\":" + userId + ",\"roomId\":" + roomId + "}}";
    }

    private TestSession connect(String sessionId, long userId, int playerNum) throws Exception {
        TestSession ts = newSession(sessionId);
        ts.emit(connectJson(userId));
        awaitTrue(() -> sessionManager.getSession(roomId, playerNum) == ts.session(), 3000,
                "세션 등록 대기 실패: " + sessionId);
        return ts;
    }

    private void seedInGameRoom() {
        GameState state = GameState.builder()
                .roomId(roomId)
                .leadingPlayer(1)
                .currentTurn(1)   // currentPlayer = PLAYER_1
                .round(1)
                .phase(GamePhase.IN_PROGRESS)
                .player1(PlayerState.builder().userId(USER_1).build())
                .player2(PlayerState.builder().userId(USER_2).build())
                .build();
        gameStateRepository.create(state).block();
        installedCardRepository.savePlayerCards(List.of(Card.JAN_1, Card.FEB_1), roomId, Player.PLAYER_1).block();
        installedCardRepository.savePlayerCards(List.of(Card.MAR_2, Card.APR_1), roomId, Player.PLAYER_2).block();
        installedCardRepository.saveRevealedCard(List.of(Card.MAY_1, Card.JUN_1), roomId).block();
        installedCardRepository.saveHiddenCard(List.of(Card.JUL_1, Card.AUG_1), roomId).block();
    }

    @Test
    @DisplayName("게임 중 이탈 시 방이 보존되고 상대에게 자동플레이 대행이 안내된다")
    void midGameDisconnectPreservesRoom() throws Exception {
        roomId = 920_001L;
        seedInGameRoom();
        TestSession p1 = connect("s1", USER_1, 1);
        TestSession p2 = connect("s2", USER_2, 2);

        p1.drop();

        awaitTrue(() -> p2.received("OPPONENT_DISCONNECTED"), 3000, "이탈 알림 미수신");
        assertTrue(p2.outbox().stream().anyMatch(m -> m.contains("자동플레이")), "대행 안내 메시지가 아님: " + p2.outbox());
        assertNotNull(gameStateRepository.findById(roomId).block(), "게임 중 이탈인데 방이 정리됨");
        assertNull(sessionManager.getSession(roomId, 1), "이탈 세션 슬롯이 비워지지 않음");
        assertSame(p2.session(), sessionManager.getSession(roomId, 2));
    }

    @Test
    @DisplayName("마지막 접속자까지 이탈하면 방 전체가 정리된다")
    void lastDisconnectTearsDownRoom() throws Exception {
        roomId = 920_002L;
        seedInGameRoom();
        TestSession p1 = connect("s1", USER_1, 1);
        TestSession p2 = connect("s2", USER_2, 2);

        p1.drop();
        awaitTrue(() -> sessionManager.getSession(roomId, 1) == null, 3000, "1차 이탈 처리 대기 실패");
        p2.drop();

        awaitTrue(() -> gameStateRepository.findById(roomId).block() == null, 3000, "방이 정리되지 않음");
        assertTrue(sessionManager.getAllUser(roomId).isEmpty());
    }

    @Test
    @DisplayName("잘못된 클라 메시지는 INVALID_REQUEST로 응답한다 (시스템 에러 아님)")
    void malformedMessagesAreRejectedAsInvalidRequest() throws Exception {
        roomId = 920_007L;
        seedInGameRoom();
        TestSession p1 = connect("s1", USER_1, 1);

        List<String> malformed = List.of(
                "not a json",
                "{\"eventType\":{\"subType\":\"NO_SUCH_TYPE\"}}",
                // payload가 필요한데 data 누락 — 예전엔 핸들러에서 NPE가 나 SYSTEM_ERROR로 새어나갔다
                "{\"eventType\":{\"subType\":\"NORMAL_SUBMIT\"}}"
        );

        for (String json : malformed) {
            p1.outbox().clear();
            p1.emit(json);
            awaitTrue(() -> p1.received("INVALID_REQUEST"), 3000, "INVALID_REQUEST 미수신: " + json);
            assertFalse(p1.received("SYSTEM_ERROR"), "클라 입력 오류가 시스템 에러로 분류됨: " + p1.outbox());
        }

        assertSame(p1.session(), sessionManager.getSession(roomId, 1), "입력 오류로 세션이 끊기면 안 된다");

        // CONNECT의 null 필드는 미접속 세션에서만 드러난다 (접속된 세션은 ALREADY_JOIN이 먼저)
        TestSession fresh = newSession("s-bad");
        fresh.emit("{\"eventType\":{\"subType\":\"CONNECT\"},\"data\":{\"userId\":null,\"roomId\":null}}");
        awaitTrue(() -> fresh.received("INVALID_REQUEST"), 3000, "CONNECT null 필드가 거절되지 않음");
        assertFalse(fresh.received("SYSTEM_ERROR"), "null 필드 언박싱 NPE가 새어나감: " + fresh.outbox());
    }

    @Test
    @DisplayName("재접속하면 세션이 복구되고 상태 스냅샷(RECONNECT_STATE)을 받는다")
    void reconnectRestoresSessionWithSnapshot() throws Exception {
        roomId = 920_003L;
        seedInGameRoom();
        TestSession p1 = connect("s1", USER_1, 1);
        TestSession p2 = connect("s2", USER_2, 2);

        p1.drop();
        awaitTrue(() -> sessionManager.getSession(roomId, 1) == null, 3000, "이탈 처리 대기 실패");

        TestSession p1b = newSession("s1b");
        p1b.emit(connectJson(USER_1));

        awaitTrue(() -> p1b.received("RECONNECT_STATE"), 3000, "스냅샷 미수신: " + p1b.outbox());
        assertSame(p1b.session(), sessionManager.getSession(roomId, 1), "세션 슬롯이 새 세션으로 복구되지 않음");
        assertTrue(p2.received("RECONNECT"), "상대가 재접속 알림을 받지 못함");

        String snapshotMsg = p1b.outbox().stream()
                .filter(m -> m.contains("\"RECONNECT_STATE\"")).findFirst().orElseThrow();
        JsonNode data = objectMapper.readTree(snapshotMsg).path("data");
        assertEquals("PLAYER_1", data.path("you").asText());
        assertEquals("IN_PROGRESS", data.path("phase").asText());
        assertEquals("PLAYER_1", data.path("currentPlayer").asText());
        assertEquals(2, data.path("myCards").size());
        assertEquals(2, data.path("opponentCardCount").asInt());
        assertEquals(2, data.path("floorCards").size(), "바닥패 월 그룹 수: " + data.path("floorCards"));
        assertTrue(data.path("remainingMs").isNumber());
    }

    @Test
    @DisplayName("같은 유저의 중복 접속은 기존 세션을 교체하고 kick한다")
    void duplicateConnectKicksOldSession() throws Exception {
        roomId = 920_004L;
        seedInGameRoom();
        TestSession p1 = connect("s1", USER_1, 1);
        connect("s2", USER_2, 2);

        TestSession p1b = newSession("s1b");
        p1b.emit(connectJson(USER_1));

        awaitTrue(() -> sessionManager.getSession(roomId, 1) == p1b.session(), 3000, "세션 교체 실패");
        awaitTrue(() -> p1.closed().get(), 3000, "낡은 세션이 kick되지 않음");

        // kick된 낡은 세션의 disconnect 처리가 새 세션을 지우면 안 된다 (identity guard)
        Thread.sleep(500);
        assertSame(p1b.session(), sessionManager.getSession(roomId, 1), "낡은 세션 정리가 새 세션을 지움");
        assertNotNull(gameStateRepository.findById(roomId).block(), "낡은 세션 정리가 방을 파괴함");
    }

    @Test
    @DisplayName("이탈한 플레이어의 턴은 자동플레이가 대행해 게임이 진행된다")
    void autoPlayCoversDisconnectedPlayersTurn() throws Exception {
        roomId = 920_005L;
        seedInGameRoom();
        TestSession p1 = connect("s1", USER_1, 1);
        TestSession p2 = connect("s2", USER_2, 2);

        p1.drop();
        awaitTrue(() -> sessionManager.getSession(roomId, 1) == null, 3000, "이탈 처리 대기 실패");

        // 12초 대기 대신 같은 TurnStep의 즉시 발사 타이머로 교체
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.IN_PROGRESS);

        awaitTrue(() -> {
            GameState gs = gameStateRepository.findById(roomId).block();
            return gs != null && gs.getCurrentTurn() == 2;
        }, 5000, "자동플레이로 턴이 진행되지 않음");
        // 턴 전환 저장은 @GameLock 안, SUBMIT_CARD 전송은 그 뒤 — 상태만 보고 단정하면 메시지보다 앞선다
        awaitTrue(() -> p2.received("SUBMIT_CARD"), 3000, "상대가 자동 제출 메시지를 받지 못함");
    }

    @Test
    @DisplayName("게임 시작 전(phase NONE) 이탈은 기존대로 방을 정리한다")
    void preGameDisconnectStillTearsDown() throws Exception {
        roomId = 920_006L;
        GameState state = GameState.builder()
                .roomId(roomId)
                .phase(GamePhase.NONE)
                .player1(PlayerState.builder().userId(USER_1).build())
                .player2(PlayerState.builder().userId(USER_2).build())
                .build();
        gameStateRepository.create(state).block();
        TestSession p1 = connect("s1", USER_1, 1);
        connect("s2", USER_2, 2);

        p1.drop();

        awaitTrue(() -> gameStateRepository.findById(roomId).block() == null, 3000, "대기실 이탈인데 방이 정리되지 않음");
    }

    @Test
    @DisplayName("게임 상태가 이미 없는 방의 이탈은 남은 세션 매핑까지 정리한다 (teardown 교차 잔여물 방지)")
    void disconnectFromDeadRoomCleansSessionMappings() throws Exception {
        roomId = 920_007L;
        // teardown과 교차한 재접속이 남긴 상태 재현: gameState 없이 세션만 방에 바인딩
        TestSession p1 = newSession("s1");
        TestSession p2 = newSession("s2");
        sessionManager.addPlayer(roomId, Player.PLAYER_1, USER_1, p1.session()).block();
        sessionManager.addPlayer(roomId, Player.PLAYER_2, USER_2, p2.session()).block();

        p1.drop();

        // removeRoom까지 탔다면 p2의 세션 매핑도 함께 정리된다
        awaitTrue(() -> sessionManager.getPlayerContext(p2.session()).block() == null, 3000,
                "죽은 방의 잔여 세션 매핑이 정리되지 않음");
        assertTrue(sessionManager.getAllUser(roomId).isEmpty());
    }

    private void awaitTrue(BooleanSupplier condition, long timeoutMillis, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        fail(message);
    }
}
