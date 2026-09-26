package com.pomingmatgo.gameservice.global.lock;

import com.pomingmatgo.gameservice.domain.service.matgo.GameActionSource;
import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.RequestEventDecoder;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.handler.websocket.GameWebSocketHandler;
import com.pomingmatgo.gameservice.api.handler.websocket.WsGameHandler;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.session.GameConnectionService;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import com.pomingmatgo.gameservice.scheduler.AutoPlayScheduler;
import com.pomingmatgo.gameservice.scheduler.RoomTimerLifecycle;
import com.pomingmatgo.gameservice.scheduler.TurnScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class InFlightExecutionLifecycleTest {
    private static final long ROOM_ID = 950_001L;
    private static final Duration TTL = Duration.ofSeconds(30);
    private final AtomicLong now = new AtomicLong();
    private final InMemoryInFlightManager flags = spy(new InMemoryInFlightManager(now::get));
    private final GameService games = mock(GameService.class);
    private final TurnFlowService flow = mock(TurnFlowService.class);
    private final WsGameHandler actions = mock(WsGameHandler.class);
    private final MessageSender sender = mock(MessageSender.class);
    private final GameConnectionService connections = mock(GameConnectionService.class);
    private final RequestEventDecoder decoder = mock(RequestEventDecoder.class);
    private final SessionManager sessions = mock(SessionManager.class);
    private final WebSocketSession session = mock(WebSocketSession.class);
    private final RoomTimerLifecycle lifecycle = new RoomTimerLifecycle();
    private final AutoPlayScheduler scheduler = new AutoPlayScheduler(lifecycle, flags, games, flow);
    private final GameWebSocketHandler handler = new GameWebSocketHandler(
            decoder, games, sessions, null, null, actions, sender, flags, connections);
    private final Sinks.Empty<Void> completion = Sinks.empty();
    private final AtomicInteger started = new AtomicInteger();
    private final AtomicInteger cancelled = new AtomicInteger();
    private VirtualTimeScheduler clock;
    private Disposable request;

    @BeforeEach
    void setUp() {
        clock = VirtualTimeScheduler.getOrSet();
        GameState state = GameState.builder().roomId(ROOM_ID).round(1).currentTurn(1)
                .leadingPlayer(1).phase(GamePhase.IN_PROGRESS).build();
        when(games.findGameState(ROOM_ID)).thenReturn(Mono.just(state));
        RequestEvent<?> event = new RequestEvent<>();
        event.setSubCategory(SubCategory.NORMAL_SUBMIT);
        WebSocketMessage message = mock(WebSocketMessage.class);
        when(message.getPayloadAsText()).thenReturn("submit");
        when(decoder.decode("submit")).thenReturn(Mono.just(event));
        when(session.receive()).thenReturn(Flux.just(message));
        when(session.getId()).thenReturn("inflight-session");
        when(sessions.getPlayerContext(session))
                .thenReturn(Mono.just(new SessionManager.PlayerContext(ROOM_ID, 1L, 1)));
        when(connections.disconnect(session)).thenReturn(Mono.empty());
        when(sender.sendPayload(eq(session), any())).thenReturn(Mono.empty());
        Mono<Void> action = completion.asMono().doOnSubscribe(ignored -> started.incrementAndGet())
                .doOnCancel(cancelled::incrementAndGet);
        when(actions.handleGameEvent(any(), eq(state), eq(Player.PLAYER_1))).thenReturn(action);
        when(flow.processNormalSubmit(eq(ROOM_ID), eq(Player.PLAYER_1), eq(0), eq(GameActionSource.AUTOPLAY), any(TurnScheduler.class)))
                .thenReturn(action);
    }

    @AfterEach
    void tearDown() {
        if (request != null) request.dispose();
        scheduler.shutdown();
        handler.shutdown();
        VirtualTimeScheduler.reset();
    }

    @ParameterizedTest
    @CsvSource({
            "false, complete, false", "false, error, false", "false, cancel, false",
            "true, complete, false", "true, error, false", "true, cancel, false",
            "false, complete, true", "false, error, true", "false, cancel, true",
            "true, complete, true", "true, error, true", "true, cancel, true"
    })
    void terminalCleanupReleasesOnlyItsOwnFlag(boolean autoplay, String terminal, boolean expired) {
        start(autoplay);
        String key = key(autoplay);
        ArgumentCaptor<String> token = ArgumentCaptor.forClass(String.class);
        verify(flags).trySetFlag(eq(key), token.capture(), eq(Duration.ofSeconds(autoplay ? 2 : 3)));
        assertEquals(1, started.get());
        assertTrue(flags.isSet(key).block());
        assertFalse(flags.trySetFlag(key, "competitor", TTL).block());
        // NORMAL과 AUTOPLAY는 각각 별도 소유자를 유지한다.
        assertTrue(flags.trySetFlag(key(!autoplay), "other-path", TTL).block());
        if (expired) {
            now.addAndGet(Duration.ofSeconds(4).toNanos());
            assertTrue(flags.trySetFlag(key, "replacement", TTL).block());
        }

        switch (terminal) {
            case "complete" -> assertEquals(Sinks.EmitResult.OK, completion.tryEmitEmpty());
            case "error" -> assertEquals(Sinks.EmitResult.OK,
                    completion.tryEmitError(new IllegalStateException("controlled action failure")));
            case "cancel" -> {
                if (autoplay) scheduler.shutdown();
                else request.dispose();
            }
            default -> fail("Unknown terminal signal");
        }

        verify(flags).deleteFlag(key, token.getValue());
        assertEquals(expired, flags.isSet(key).block());
        assertTrue(flags.isSet(key(!autoplay)).block());
        assertEquals(terminal.equals("cancel") ? 1 : 0, cancelled.get());
        if (expired) {
            assertFalse(flags.trySetFlag(key, "next", TTL).block());
            flags.deleteFlag(key, "replacement").block();
        }
        assertTrue(flags.trySetFlag(key, "next", TTL).block());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejectedAcquisitionDoesNotReleaseExistingOwnersFlag(boolean autoplay) {
        assertTrue(flags.trySetFlag(key(autoplay), "existing", TTL).block());
        start(autoplay);
        assertEquals(0, started.get());
        verify(flags, never()).deleteFlag(anyString(), anyString());
        assertTrue(flags.isSet(key(autoplay)).block());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void synchronousActionCreationFailureAlsoReleasesFlag(boolean autoplay) {
        IllegalStateException failure = new IllegalStateException("controlled creation failure");
        if (autoplay) {
            when(flow.processNormalSubmit(anyLong(), any(), anyInt(), eq(GameActionSource.AUTOPLAY), any())).thenThrow(failure);
        } else {
            when(actions.handleGameEvent(any(), any(), any())).thenThrow(failure);
        }
        start(autoplay);
        assertEquals(0, started.get());
        verify(flags).deleteFlag(eq(key(autoplay)), anyString());
        assertFalse(flags.isSet(key(autoplay)).block());
        assertTrue(flags.trySetFlag(key(autoplay), "next", TTL).block());
    }

    private void start(boolean autoplay) {
        if (autoplay) {
            lifecycle.open(ROOM_ID);
            scheduler.scheduleAutoPlay(ROOM_ID, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.IN_PROGRESS);
            clock.advanceTimeBy(Duration.ofMillis(100));
        } else {
            request = handler.handle(session).subscribe();
        }
    }

    private String key(boolean autoplay) {
        return autoplay ? InFlightManager.autoplayKey(ROOM_ID, 1) : InFlightManager.normalKey(ROOM_ID, 1);
    }
}
