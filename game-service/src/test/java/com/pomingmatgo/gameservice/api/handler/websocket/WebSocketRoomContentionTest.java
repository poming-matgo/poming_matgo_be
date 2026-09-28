package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.application.connection.GameConnectionService;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
        "spring.profiles.active=in-memory",
        "spring.autoconfigure.exclude=org.redisson.spring.starter.RedissonAutoConfigurationV2,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration"
})
class WebSocketRoomContentionTest {
    private static final long ROOM_ID = 960_058L;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    @Autowired GameWebSocketHandler handler;
    @Autowired GameStateRepository states;
    @Autowired LeadingPlayerRepository leaders;
    @Autowired SessionManager sessions;
    @Autowired InMemoryRoomExecutionGate gate;
    @Autowired RoomCleanupService cleanup;
    @Autowired ObjectMapper mapper;
    @MockBean GameConnectionService connections;
    private InMemoryRoomExecutionGate.Entry occupied;

    @AfterEach
    void tearDown() {
        releaseGate();
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
    }

    @ParameterizedTest(name = "선플레이어 선택={0}")
    @ValueSource(booleans = {false, true})
    void contentionIsSerializedWithoutClosingReceiveAndExplicitResubmissionSucceeds(boolean selection) {
        GameState initial = GameState.createEmptyRoom(ROOM_ID).join(1L).join(2L);
        if (selection) initial = initial.toBuilder().phase(GamePhase.DETERMINING_STARTING_PLAYER).build();
        states.create(initial).block(TIMEOUT);
        leaders.saveSelectedCard(List.of(Card.JAN_1, Card.FEB_1), ROOM_ID).block(TIMEOUT);
        WebSocketSession session = mock(WebSocketSession.class);
        Sinks.Many<WebSocketMessage> incoming = Sinks.many().unicast().onBackpressureBuffer();
        List<JsonNode> responses = new ArrayList<>();
        when(session.getId()).thenReturn("room-contention");
        when(session.isOpen()).thenReturn(true);
        when(session.receive()).thenReturn(incoming.asFlux());
        when(session.textMessage(anyString())).thenAnswer(invocation -> text(invocation.getArgument(0)));
        when(session.send(any())).thenAnswer(invocation -> Flux.from(
                        invocation.<Publisher<WebSocketMessage>>getArgument(0))
                .flatMap(message -> Mono.fromCallable(() -> mapper.readTree(message.getPayloadAsText())))
                .doOnNext(responses::add).then());
        when(connections.disconnect(any())).thenReturn(Mono.empty());
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, session).block(TIMEOUT);
        String request = selection
                ? "{\"eventType\":{\"type\":\"PREGAME\",\"subType\":\"LEADER_SELECTION\"},\"data\":{\"cardIndex\":0}}"
                : "{\"eventType\":{\"type\":\"ROOM\",\"subType\":\"READY\"}}";
        occupied = gate.acquire(ROOM_ID);

        // 송수신 세션만 대역이며 디코더·라우팅·Spring AOP·저장소·오류 직렬화는 실제 구현이다.
        StepVerifier.create(handler.handle(session))
                .then(() -> assertEquals(Sinks.EmitResult.OK, incoming.tryEmitNext(text(request))))
                .then(() -> {
                    assertEquals(1, responses.size());
                    assertEquals("TRY_AGAIN", responses.get(0).path("errorCode").asText());
                    assertFalse(responses.get(0).path("errorMessage").asText().isBlank());
                    assertFalse(states.findById(ROOM_ID).block(TIMEOUT).getPlayer1().isReady());
                    assertEquals(0, leaders.getPlayerSelectedCard(ROOM_ID).block(TIMEOUT).getPlayer1Month());
                    verify(session, never()).close();
                    releaseGate();
                })
                // 동일 방을 유지한 테스트에서 명시적으로 다시 보낸다. 운영 자동 재시도 보장이 아니다.
                .then(() -> assertEquals(Sinks.EmitResult.OK, incoming.tryEmitNext(text(request))))
                .then(() -> {
                    assertEquals(2, responses.size());
                    assertFalse(responses.get(1).has("errorCode"));
                    if (selection) assertEquals(1, leaders.getPlayerSelectedCard(ROOM_ID).block(TIMEOUT).getPlayer1Month());
                    else assertTrue(states.findById(ROOM_ID).block(TIMEOUT).getPlayer1().isReady());
                })
                .thenCancel().verify(TIMEOUT);
    }

    private void releaseGate() {
        if (occupied != null) {
            gate.release(occupied);
            occupied = null;
        }
    }

    private WebSocketMessage text(String payload) {
        return new WebSocketMessage(WebSocketMessage.Type.TEXT,
                DefaultDataBufferFactory.sharedInstance.wrap(payload.getBytes(StandardCharsets.UTF_8)));
    }
}
