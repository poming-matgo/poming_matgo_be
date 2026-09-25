package com.pomingmatgo.gameservice.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.RequestEventDecoder;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.handler.websocket.GameWebSocketHandler;
import com.pomingmatgo.gameservice.api.handler.websocket.WsGameHandler;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import com.pomingmatgo.gameservice.global.lock.InMemoryInFlightManager;
import com.pomingmatgo.gameservice.global.session.GameConnectionService;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.websocket.WebsocketOutbound;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WebSocketDisconnectPropagationTest {
    private static final long ROOM_ID = 960_001L;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private final SessionManager sessions = new SessionManager();
    private final InMemoryInFlightManager flags = spy(new InMemoryInFlightManager());
    private final GameConnectionService connections = mock(GameConnectionService.class);
    private final Sinks.Empty<Void> started = Sinks.empty();
    private final Sinks.Empty<Void> disconnected = Sinks.empty();
    private final Sinks.Empty<Void> clientEnded = Sinks.empty();
    private final Sinks.Empty<Void> actionCompletion = Sinks.empty();
    private final Sinks.One<SignalType> actionEnded = Sinks.one();
    private final Sinks.One<SignalType> handlerEnded = Sinks.one();
    private final AtomicReference<Connection> connection = new AtomicReference<>();
    private final AtomicReference<Connection> serverConnection = new AtomicReference<>();
    private final AtomicReference<WebsocketOutbound> outbound = new AtomicReference<>();
    private final AtomicReference<WebSocketSession> serverSession = new AtomicReference<>();
    private final AtomicReference<Throwable> clientFailure = new AtomicReference<>();
    private final AtomicReference<Throwable> handlerFailure = new AtomicReference<>();
    private GameWebSocketHandler handler;
    private DisposableServer server;
    private Disposable client;

    @AfterEach
    void tearDown() {
        actionCompletion.tryEmitEmpty();
        if (connection.get() != null) connection.get().dispose();
        if (client != null) client.dispose();
        if (server != null) server.disposeNow(TIMEOUT);
        if (handler != null) handler.shutdown();
        sessions.shutdown();
    }

    @ParameterizedTest
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    void closeFrameWaitsForActionButTransportLossCancelsIt(boolean closeFrame, boolean pendingAction) {
        startServer(pendingAction);
        client = HttpClient.create().host("127.0.0.1").port(server.port()).websocket()
                .uri("/gostop")
                .handle((in, out) -> {
                    in.withConnection(connection::set);
                    outbound.set(out);
                    return out.sendString(Mono.just("submit")).then().then(in.receive().then());
                }).doFinally(ignored -> clientEnded.tryEmitEmpty())
                .subscribe(ignored -> {}, clientFailure::set);

        started.asMono().block(TIMEOUT);
        String key = InFlightManager.normalKey(ROOM_ID, 1);
        if (pendingAction) {
            assertTrue(flags.isSet(key).block(TIMEOUT));
        } else {
            assertEquals(SignalType.ON_COMPLETE, actionEnded.asMono().block(TIMEOUT));
        }

        if (closeFrame) outbound.get().sendClose().block(TIMEOUT);
        else connection.get().channel().close();

        // closeStatus는 채널 비활성화 전에 완료될 수 있으므로 실제 채널 종료를 기다린다.
        serverConnection.get().onDispose().block(TIMEOUT);
        assertFalse(serverSession.get().isOpen());
        if (pendingAction && closeFrame) {
            // 정상 receive 완료는 concatMap 내부의 처리 중 액션을 기다린다.
            assertEquals(1, actionCompletion.currentSubscriberCount());
            verify(connections, never()).disconnect(any());
            verify(flags, never()).deleteFlag(anyString(), anyString());
            assertEquals(Sinks.EmitResult.OK, actionCompletion.tryEmitEmpty());
        }
        assertEquals(pendingAction && !closeFrame ? SignalType.CANCEL : SignalType.ON_COMPLETE,
                actionEnded.asMono().block(TIMEOUT));
        assertEquals(closeFrame ? SignalType.ON_COMPLETE : SignalType.ON_ERROR,
                handlerEnded.asMono().block(TIMEOUT));
        disconnected.asMono().block(TIMEOUT);
        verify(connections, times(1)).disconnect(any());
        verify(flags, times(1)).deleteFlag(eq(key), anyString());
        assertFalse(flags.isSet(key).block(TIMEOUT));
        assertTrue(flags.trySetFlag(key, "next", Duration.ofSeconds(3)).block(TIMEOUT));
        assertEquals(0, actionCompletion.currentSubscriberCount());
        if (closeFrame) assertNull(handlerFailure.get());
        else assertNotNull(handlerFailure.get());
        clientEnded.asMono().block(TIMEOUT);
        assertNull(clientFailure.get());
    }

    private void startServer(boolean pendingAction) {
        RequestEventDecoder decoder = mock(RequestEventDecoder.class);
        GameService games = mock(GameService.class);
        WsGameHandler actions = mock(WsGameHandler.class);
        RequestEvent<?> event = new RequestEvent<>();
        event.setSubCategory(SubCategory.NORMAL_SUBMIT);
        when(decoder.decode("submit")).thenReturn(Mono.just(event));
        GameState state = GameState.builder().roomId(ROOM_ID).round(1).currentTurn(1)
                .leadingPlayer(1).phase(GamePhase.IN_PROGRESS).build();
        when(games.findGameState(ROOM_ID)).thenReturn(Mono.just(state));
        Mono<Void> action = (pendingAction ? actionCompletion.asMono() : Mono.<Void>empty())
                .doOnSubscribe(ignored -> started.tryEmitEmpty())
                .doFinally(actionEnded::tryEmitValue);
        when(actions.handleGameEvent(any(), eq(state), eq(Player.PLAYER_1))).thenReturn(action);
        when(connections.disconnect(any())).thenReturn(Mono.fromRunnable(() -> disconnected.tryEmitEmpty()));
        handler = new GameWebSocketHandler(decoder, games, sessions, null, null, actions,
                mock(MessageSender.class), flags, connections);
        HandshakeWebSocketService upgrade = new HandshakeWebSocketService();
        // 소켓·수신·핸들러·InFlight는 실제 구현, 게임 처리 완료만 제어한다.
        var httpHandler = WebHttpHandlerBuilder.webHandler(exchange -> upgrade.handleRequest(exchange,
                session -> {
                    serverSession.set(session);
                    return sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, session)
                        .then(handler.handle(session))
                        .doOnError(handlerFailure::set)
                        .doFinally(handlerEnded::tryEmitValue);
                })).build();
        server = HttpServer.create().host("127.0.0.1").port(0)
                .doOnConnection(serverConnection::set)
                .handle(new ReactorHttpHandlerAdapter(httpHandler)).bindNow(TIMEOUT);
    }
}
