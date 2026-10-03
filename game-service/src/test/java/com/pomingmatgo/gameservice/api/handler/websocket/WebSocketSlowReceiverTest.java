package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.application.game.InMemoryGameActionExecutor;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryAcquiredCardRepository;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryGameStateRepository;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryInstalledCardRepository;
import com.pomingmatgo.gameservice.infrastructure.repository.inmemory.InMemoryLeadingPlayerRepository;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
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
import java.io.DataInputStream;
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

    enum AfterCleanup { RESUME_READING, CALLER_CANCEL_THEN_RESUME_READING, PEER_RESET }

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

    @ParameterizedTest
    @EnumSource(AfterCleanup.class)
    void roomCleanupDoesNotRecallAnAlreadyStartedTcpFrame(AfterCleanup followup) throws Exception {
        long roomId = 970_002L;
        WebSocketSession session = connectWithoutReadingFrames();
        var executionGate = new InMemoryRoomExecutionGate();
        var state = new InMemoryGameStateRepository(new RoomTimerLifecycle(), executionGate);
        var executor = new InMemoryGameActionExecutor(executionGate, event -> {});
        var cleanup = new RoomCleanupService(state, new InMemoryInstalledCardRepository(),
                new InMemoryAcquiredCardRepository(), new InMemoryLeadingPlayerRepository(),
                executor, event -> {}, sessions);
        @SuppressWarnings("unchecked")
        ObjectProvider<ThroughputRecorder> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(recorder);
        MessageSender sender = new MessageSender(new ObjectMapper(), sessions, provider);
        Sinks.One<SignalType> notificationEnded = Sinks.one();
        try {
            state.create(GameState.createEmptyRoom(roomId)).block(TIMEOUT);
            sessions.addPlayer(roomId, Player.PLAYER_1, 101L, session).block(TIMEOUT);
            var recipients = sender.captureRecipients(roomId);
            String payload = "x".repeat(PAYLOAD_BYTES);
            send = sender.sendPayload(session, payload).contextWrite(recipients)
                    .doFinally(sendEnded::tryEmitValue)
                    .subscribe(ignored -> {}, sendFailure::set);
            TransportState stalled = awaitStalledWrite();

            // 종료 안내를 포함한 정리 서비스 경계다. WS 선택/disconnect 전체 경로 재현은 아니다.
            cleanup.cleanupRoom(roomId, sender.sendPayload(session, "room-closed")
                    .doFinally(notificationEnded::tryEmitValue)).block(TIMEOUT);
            assertEquals(SignalType.CANCEL, notificationEnded.asMono().block(TIMEOUT));
            assertNull(state.findById(roomId).block(TIMEOUT));
            assertNull(sessions.getPlayerContext(session).block(TIMEOUT));
            assertTrue(sessions.getAllUser(roomId).isEmpty());
            assertTrue(session.isOpen());
            assertFalse(send.isDisposed());
            TransportState afterCleanup = awaitStalledWrite();
            assertEquals(1, recorder.snapshot().totalCancelled());
            assertEquals(0, recorder.snapshot().totalSent());

            // 정리 이후 아직 시작하지 않은 같은 액션의 안내는 기존 수신자 검사로 제외한다.
            sender.sendPayload(session, "must-be-skipped").contextWrite(recipients).block(TIMEOUT);
            assertEquals(1, recorder.snapshot().totalSkipped());

            if (followup == AfterCleanup.CALLER_CANCEL_THEN_RESUME_READING) {
                send.dispose();
                assertEquals(SignalType.CANCEL, sendEnded.asMono().block(TIMEOUT));
                assertEquals(2, recorder.snapshot().totalCancelled());
                assertTrue(transportState().pendingBytes() > 0);
            }
            if (followup == AfterCleanup.PEER_RESET) {
                peer.setSoLinger(true, 0);
                peer.close();
                connection.get().onDispose().block(TIMEOUT);
                handlerEnded.asMono().block(TIMEOUT);
                assertEquals(SignalType.ON_COMPLETE, sendEnded.asMono().block(TIMEOUT));
                assertEquals(1, recorder.snapshot().totalSent());
                assertFalse(session.isOpen());
            } else {
                // TCP에 이미 들어간 프레임은 Publisher 취소 뒤에도 수신될 수 있다.
                assertEquals("\"" + payload + "\"", readTextFrame());
                assertEquals("\"room-closed\"", readTextFrame());
                SignalType terminal = sendEnded.asMono().block(TIMEOUT);
                assertEquals(followup == AfterCleanup.RESUME_READING
                        ? SignalType.ON_COMPLETE : SignalType.CANCEL, terminal);
                assertEquals(followup == AfterCleanup.RESUME_READING ? 1 : 0,
                        recorder.snapshot().totalSent());
                assertTrue(session.isOpen());
            }
            assertEquals(0, transportState().pendingBytes());
            assertNull(sendFailure.get());
            assertEquals(0, recorder.snapshot().totalFailed());
            System.out.printf("cleanup/%s: stalled=%s, afterCleanup=%s, final=%s, outcome=%s%n",
                    followup, stalled, afterCleanup, transportState(), recorder.snapshot());
        } finally {
            cleanup.shutdown();
            executor.shutdown();
        }
    }

    private String readTextFrame() throws Exception {
        DataInputStream input = new DataInputStream(peer.getInputStream());
        assertEquals(0x81, input.readUnsignedByte(), "단일 FIN text frame");
        int lengthByte = input.readUnsignedByte();
        assertEquals(0, lengthByte & 0x80, "서버 프레임은 마스킹하지 않는다");
        long length = lengthByte & 0x7f;
        if (length == 126) length = input.readUnsignedShort();
        else if (length == 127) length = input.readLong();
        assertTrue(length >= 0 && length <= PAYLOAD_BYTES + 2L, "프레임 길이 상한");
        byte[] body = new byte[(int) length];
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        int offset = 0;
        while (offset < body.length) {
            long remaining = deadline - System.nanoTime();
            assertTrue(remaining > 0, "프레임 읽기 총 시간 상한 초과");
            peer.setSoTimeout((int) Math.max(1, Duration.ofNanos(remaining).toMillis()));
            int read = input.read(body, offset, body.length - offset);
            assertNotEquals(-1, read, "프레임 본문 도중 연결 종료");
            offset += read;
        }
        peer.setSoTimeout((int) TIMEOUT.toMillis());
        return new String(body, StandardCharsets.UTF_8);
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
