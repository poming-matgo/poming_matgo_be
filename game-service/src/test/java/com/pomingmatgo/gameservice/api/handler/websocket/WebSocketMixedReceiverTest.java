package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.messaging.ResponseEvent;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import com.pomingmatgo.gameservice.infrastructure.messaging.MessageSender;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WebSocketMixedReceiverTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final int MAX_FRAMES = 1024;
    private static final long MIXED_ROOM = 980_001L;
    private static final long NORMAL_ROOM = 980_002L;
    private final ObjectMapper mapper = new ObjectMapper();
    private final SessionManager sessions = new SessionManager();
    private final ThroughputRecorder recorder = new ThroughputRecorder();
    private final List<Peer> peers = new ArrayList<>();
    private final AtomicReference<Connection> accepted = new AtomicReference<>();
    private final AtomicReference<Peer> handshaking = new AtomicReference<>();
    private final List<CompletableFuture<Void>> sends = new ArrayList<>();
    private DisposableServer server;

    @AfterEach
    void tearDown() throws Exception {
        sends.forEach(send -> send.cancel(true));
        for (Peer peer : peers) {
            peer.socket.close();
            if (peer.connection != null) {
                peer.connection.dispose();
                peer.connection.onDispose().block(TIMEOUT);
            }
            if (peer.session != null) peer.ended.asMono().block(TIMEOUT);
        }
        if (server != null) server.disposeNow(TIMEOUT);
        sessions.shutdown();
    }

    @ParameterizedTest(name = "pauseReading={0}")
    @ValueSource(booleans = {false, true})
    void normalSizeBroadcastSeparatesSameRoomCompletionFromOtherRoomProgress(boolean pauseReading) throws Exception {
        startServer();
        Peer target = connect();
        Peer partner = connect();
        Peer otherFirst = connect();
        Peer otherSecond = connect();
        register(MIXED_ROOM, target, partner);
        register(NORMAL_ROOM, otherFirst, otherSecond);
        @SuppressWarnings("unchecked")
        ObjectProvider<ThroughputRecorder> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(recorder);
        MessageSender sender = new MessageSender(mapper, sessions, provider);

        long began = System.nanoTime();
        long deadline = began + Duration.ofSeconds(20).toNanos();
        long partnerMaxNanos = 0;
        int submitted = 0;
        int payloadBytes = mapper.writeValueAsBytes(payload(0)).length;
        CompletableFuture<Void> pending = null;
        // 실제 DTO 크기의 유한 송신 계층 실험이다. 게임 phase 전이·운영 액션 빈도는 재현하지 않는다.
        for (int i = 0; i < MAX_FRAMES; i++) {
            assertTrue(System.nanoTime() < deadline, "반복 송신 시간 상한");
            long sentAt = System.nanoTime();
            pending = send(sender, MIXED_ROOM, payload(i));
            submitted++;
            assertEquals(mapper.writeValueAsString(payload(i)), partner.read());
            partnerMaxNanos = Math.max(partnerMaxNanos, System.nanoTime() - sentAt);
            if (!pauseReading) assertEquals(mapper.writeValueAsString(payload(i)), target.read());
            try {
                pending.get(pauseReading ? 200 : TIMEOUT.toMillis(), MILLISECONDS);
            } catch (TimeoutException waiting) {
                assertTrue(pauseReading, "정상 수신 대조군 송신 대기");
                TransportState stalled = target.state();
                assertTrue(stalled.active());
                assertTrue(stalled.pendingBytes() > 0, "실제 Netty 쓰기 대기 확인");
                break;
            }
        }
        assertNotNull(pending);
        TransportState beforeProbes = target.state();
        if (pauseReading) {
            assertFalse(pending.isDone(), "제한된 전송량 안에서 실제 쓰기 대기에 도달해야 한다");
            assertTrue(beforeProbes.pendingBytes() > 0);
        } else {
            assertEquals(MAX_FRAMES, submitted);
            assertTrue(pending.isDone());
            assertEquals(0, beforeProbes.pendingBytes());
        }

        // 기존 broadcast.then(후속 안내) 의존성을 실제 전송 완료에 연결한다.
        var followupPayload = WebSocketResDto.of(Player.PLAYER_1, ResponseEvent.CARD_REVEALED,
                "상단 카드 정보", Card.DEC_4);
        CompletableFuture<Void> followup = Mono.fromFuture(pending, true)
                .then(sender.sendMessageToAllUser(MIXED_ROOM, followupPayload)).toFuture();
        sends.add(followup);
        List<Long> otherNanos = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            long sentAt = System.nanoTime();
            CompletableFuture<Void> probe = send(sender, NORMAL_ROOM, payload(i));
            assertEquals(mapper.writeValueAsString(payload(i)), otherFirst.read());
            assertEquals(mapper.writeValueAsString(payload(i)), otherSecond.read());
            probe.get(TIMEOUT.toMillis(), MILLISECONDS);
            otherNanos.add(System.nanoTime() - sentAt);
        }
        TransportState afterProbes = target.state();
        if (pauseReading) {
            assertFalse(pending.isDone());
            assertFalse(followup.isDone());
            assertTrue(afterProbes.pendingBytes() > 0);
            partner.socket.setSoTimeout(200);
            assertThrows(SocketTimeoutException.class, partner::read,
                    "현재 브로드캐스트는 상대에게 도착해도 후속 안내는 느린 수신자를 기다린다");
            partner.socket.setSoTimeout((int) TIMEOUT.toMillis());
            for (int i = 0; i < submitted; i++) {
                assertEquals(mapper.writeValueAsString(payload(i)), target.read());
            }
        }
        assertEquals(mapper.writeValueAsString(followupPayload), target.read());
        assertEquals(mapper.writeValueAsString(followupPayload), partner.read());
        followup.get(TIMEOUT.toMillis(), MILLISECONDS);
        assertEquals(0, target.state().pendingBytes());
        assertTrue(target.session.isOpen());
        assertEquals(2L * (submitted + 1 + 16), recorder.snapshot().totalSent());
        assertEquals(0, recorder.snapshot().totalFailed());
        assertEquals(0, recorder.snapshot().totalSkipped());
        assertEquals(0, recorder.snapshot().totalCancelled());
        otherNanos.sort(Long::compare);
        System.out.printf("mixed pause=%s frames=%d jsonBytes=%d elapsedMs=%.3f partnerMaxMs=%.3f "
                        + "otherSamples=%d otherMedianMs=%.3f otherMaxMs=%.3f before=%s after=%s drained=%s%n",
                pauseReading, submitted, payloadBytes, (System.nanoTime() - began) / 1e6,
                partnerMaxNanos / 1e6, otherNanos.size(), otherNanos.get(8) / 1e6,
                otherNanos.get(15) / 1e6, beforeProbes, afterProbes, target.state());
    }

    private WebSocketResDto<Card> payload(int index) {
        return WebSocketResDto.of(Player.PLAYER_1, ResponseEvent.SUBMIT_CARD,
                "카드 제출", Card.values()[index % Card.values().length]);
    }

    private CompletableFuture<Void> send(MessageSender sender, long roomId, WebSocketResDto<?> payload) {
        CompletableFuture<Void> future = sender.sendMessageToAllUser(roomId, payload).toFuture();
        sends.add(future);
        return future;
    }

    private void register(long roomId, Peer first, Peer second) {
        sessions.addPlayer(roomId, Player.PLAYER_1, 101L, first.session).block(TIMEOUT);
        sessions.addPlayer(roomId, Player.PLAYER_2, 102L, second.session).block(TIMEOUT);
    }

    private void startServer() {
        HandshakeWebSocketService upgrade = new HandshakeWebSocketService();
        var handler = WebHttpHandlerBuilder.webHandler(exchange -> upgrade.handleRequest(exchange, session -> {
            Peer peer = handshaking.get();
            peer.ready.tryEmitValue(session);
            return session.receive().then().doFinally(signal -> peer.ended.tryEmitValue(true));
        })).build();
        server = HttpServer.create().host("127.0.0.1").port(0)
                .childOption(ChannelOption.SO_SNDBUF, 4096)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(4096, 8192))
                .doOnConnection(accepted::set)
                .handle(new ReactorHttpHandlerAdapter(handler)).bindNow(TIMEOUT);
    }

    private Peer connect() throws Exception {
        Peer peer = new Peer();
        peers.add(peer);
        handshaking.set(peer);
        peer.socket.setReceiveBufferSize(1024);
        peer.socket.setSoTimeout((int) TIMEOUT.toMillis());
        peer.socket.connect(new InetSocketAddress("127.0.0.1", server.port()), (int) TIMEOUT.toMillis());
        String request = "GET /mixed HTTP/1.1\r\nHost: 127.0.0.1:" + server.port()
                + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
        peer.socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
        peer.socket.getOutputStream().flush();
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        while (response.size() < 8192) {
            int next = peer.socket.getInputStream().read();
            assertNotEquals(-1, next);
            response.write(next);
            if (response.toString(StandardCharsets.US_ASCII).endsWith("\r\n\r\n")) break;
        }
        String headers = response.toString(StandardCharsets.US_ASCII);
        assertTrue(headers.startsWith("HTTP/1.1 101"), headers);
        assertTrue(headers.endsWith("\r\n\r\n"));
        assertTrue(headers.contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        peer.session = peer.ready.asMono().block(TIMEOUT);
        peer.connection = accepted.get();
        return peer;
    }

    private static class Peer {
        final Socket socket = new Socket();
        final Sinks.One<Boolean> ended = Sinks.one();
        final Sinks.One<WebSocketSession> ready = Sinks.one();
        Connection connection;
        WebSocketSession session;

        String read() throws Exception {
            DataInputStream input = new DataInputStream(socket.getInputStream());
            assertEquals(0x81, input.readUnsignedByte());
            int lengthByte = input.readUnsignedByte();
            assertEquals(0, lengthByte & 0x80);
            int length = lengthByte & 0x7f;
            if (length == 126) length = input.readUnsignedShort();
            else assertNotEquals(127, length, "이 실험은 작은 프레임만 사용한다");
            assertTrue(length <= 1024);
            byte[] body = input.readNBytes(length);
            assertEquals(length, body.length);
            return new String(body, StandardCharsets.UTF_8);
        }

        TransportState state() {
            var channel = connection.channel();
            return Mono.<TransportState>create(sink -> channel.eventLoop().execute(() -> {
                var buffer = channel.unsafe().outboundBuffer();
                sink.success(new TransportState(channel.isActive(), channel.isWritable(),
                        buffer == null ? 0 : buffer.totalPendingWriteBytes()));
            })).block(TIMEOUT);
        }
    }

    private record TransportState(boolean active, boolean writable, long pendingBytes) {}
}
