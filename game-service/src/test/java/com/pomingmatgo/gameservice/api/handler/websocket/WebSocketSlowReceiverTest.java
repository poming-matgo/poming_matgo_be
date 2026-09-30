package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import com.pomingmatgo.gameservice.infrastructure.messaging.MessageSender;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.ObjectProvider;
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
import reactor.netty.http.server.HttpServer;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WebSocketSlowReceiverTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final int PAYLOAD_BYTES = 8 * 1024 * 1024;
    private final SessionManager sessions = new SessionManager();
    private final ThroughputRecorder recorder = new ThroughputRecorder();
    private final Sinks.One<WebSocketSession> connected = Sinks.one();
    private final Sinks.One<SignalType> handlerEnded = Sinks.one();
    private final Sinks.One<SignalType> sendEnded = Sinks.one();
    private final AtomicReference<Connection> connection = new AtomicReference<>();
    private final AtomicReference<Throwable> sendFailure = new AtomicReference<>();
    private DisposableServer server;
    private Socket peer;
    private Disposable send;

    enum Termination { PEER_RESET, CALLER_CANCEL_THEN_PEER_RESET }

    @AfterEach
    void tearDown() throws Exception {
        if (send != null) send.dispose();
        if (peer != null) peer.close();
        if (connection.get() != null) connection.get().dispose();
        if (server != null) server.disposeNow(TIMEOUT);
        sessions.shutdown();
    }

    @ParameterizedTest
    @EnumSource(Termination.class)
    void stalledTcpWriteTerminatesAndChannelCloseReleasesOutboundBuffer(Termination termination) throws Exception {
        WebSocketSession session = connectWithoutReadingFrames();
        @SuppressWarnings("unchecked")
        ObjectProvider<ThroughputRecorder> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(recorder);
        MessageSender sender = new MessageSender(new ObjectMapper(), sessions, provider);

        // 유한한 큰 프레임으로 실제 TCP 쓰기 대기를 만든다. 정상 게임 메시지 크기의 부하 실험은 아니다.
        send = sender.sendPayload(session, "x".repeat(PAYLOAD_BYTES))
                .doFinally(sendEnded::tryEmitValue)
                .subscribe(ignored -> {}, sendFailure::set);
        TransportState stalled = awaitStalledWrite();
        assertTrue(stalled.pendingBytes() > 0);
        assertFalse(stalled.writable());
        assertTrue(stalled.active());
        assertEquals(0, recorder.snapshot().totalSent());
        assertEquals(0, recorder.snapshot().totalFailed());
        assertEquals(0, recorder.snapshot().totalCancelled());

        if (termination == Termination.CALLER_CANCEL_THEN_PEER_RESET) {
            send.dispose();
            assertEquals(SignalType.CANCEL, sendEnded.asMono().block(TIMEOUT));
            assertEquals(1, recorder.snapshot().totalCancelled());
            TransportState afterCancel = transportState();
            // 취소 신호와 채널 버퍼 해제는 별도 관측한다. 취소만으로 연결 종료를 요구하지 않는다.
            System.out.printf("%s: stalled=%s, afterCancel=%s%n", termination, stalled, afterCancel);
        }

        // close frame도 읽지 않는 peer를 실제 TCP RST로 종료한다.
        peer.setSoLinger(true, 0);
        peer.close();
        connection.get().onDispose().block(TIMEOUT);
        handlerEnded.asMono().block(TIMEOUT);
        SignalType terminal = sendEnded.asMono().block(TIMEOUT);
        if (termination == Termination.PEER_RESET) {
            // 전송 계층이 종료를 정상 완료로 전달할 수도 있다. 클라이언트 수신 성공으로 해석하지 않는다.
            assertEquals(SignalType.ON_COMPLETE, terminal);
            assertEquals(1, recorder.snapshot().totalSent() + recorder.snapshot().totalFailed());
            assertEquals(0, recorder.snapshot().totalCancelled());
        } else {
            assertEquals(0, recorder.snapshot().totalFailed());
            assertEquals(0, recorder.snapshot().totalSent());
        }
        TransportState closed = transportState();
        assertFalse(closed.active());
        assertEquals(0, closed.pendingBytes());
        assertFalse(session.isOpen());
        assertNull(sendFailure.get());
        assertEquals(0, recorder.snapshot().totalSkipped());
        System.out.printf("%s: stalled=%s, closed=%s, outcome=%s%n",
                termination, stalled, closed, recorder.snapshot());
    }

    private WebSocketSession connectWithoutReadingFrames() throws Exception {
        HandshakeWebSocketService upgrade = new HandshakeWebSocketService();
        var handler = WebHttpHandlerBuilder.webHandler(exchange -> upgrade.handleRequest(exchange, session -> {
            connected.tryEmitValue(session);
            return session.receive().then().doFinally(handlerEnded::tryEmitValue);
        })).build();
        server = HttpServer.create().host("127.0.0.1").port(0)
                .childOption(ChannelOption.SO_SNDBUF, 4096)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(4096, 8192))
                .doOnConnection(connection::set)
                .handle(new ReactorHttpHandlerAdapter(handler)).bindNow(TIMEOUT);

        peer = new Socket();
        peer.setReceiveBufferSize(1024);
        peer.setSoTimeout((int) TIMEOUT.toMillis());
        peer.connect(new InetSocketAddress("127.0.0.1", server.port()), (int) TIMEOUT.toMillis());
        String request = "GET /slow HTTP/1.1\r\nHost: 127.0.0.1:" + server.port()
                + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
        peer.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
        peer.getOutputStream().flush();
        // 헤더만 한 바이트씩 읽어 프레임을 미리 읽는 클라이언트 버퍼를 만들지 않는다.
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        while (response.size() < 8192) {
            int next = peer.getInputStream().read();
            assertNotEquals(-1, next, "WebSocket handshake 도중 연결 종료");
            response.write(next);
            if (response.toString(StandardCharsets.US_ASCII).endsWith("\r\n\r\n")) break;
        }
        String headers = response.toString(StandardCharsets.US_ASCII);
        assertTrue(headers.startsWith("HTTP/1.1 101"), headers);
        assertTrue(headers.endsWith("\r\n\r\n"), "handshake 헤더 크기 상한 초과");
        assertTrue(headers.contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), headers);
        return connected.asMono().block(TIMEOUT);
    }

    private TransportState awaitStalledWrite() throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long stalledSince = 0;
        TransportState state;
        do {
            state = transportState();
            if (state.active() && !state.writable() && state.pendingBytes() > 0 && !send.isDisposed()) {
                if (stalledSince == 0) stalledSince = System.nanoTime();
                if (System.nanoTime() - stalledSince >= Duration.ofMillis(200).toNanos()) return state;
            } else {
                stalledSince = 0;
            }
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);
        fail("TCP 쓰기 대기 미확인: " + state + ", outcome=" + recorder.snapshot());
        return state;
    }

    private TransportState transportState() {
        var channel = connection.get().channel();
        // Netty 내부 버퍼는 소유 이벤트 루프에서 읽고 수치만 테스트 스레드로 전달한다.
        return Mono.<TransportState>create(sink -> channel.eventLoop().execute(() -> {
            var buffer = channel.unsafe().outboundBuffer();
            sink.success(new TransportState(channel.isActive(), channel.isWritable(),
                    buffer == null ? 0 : buffer.totalPendingWriteBytes()));
        })).block(TIMEOUT);
    }

    private record TransportState(boolean active, boolean writable, long pendingBytes) {}
}
