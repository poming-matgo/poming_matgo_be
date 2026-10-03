package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.application.game.*;
import com.pomingmatgo.gameservice.application.pregame.PreGameService;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.*;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.infrastructure.scheduler.TurnScheduler;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.reactive.socket.*;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@ActiveProfiles("in-memory")
class GameTrafficSocketTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final long ROOM = 981_001L;
    @Autowired TurnFlowService turns;
    @Autowired GamePlayService gameplay;
    @Autowired PreGameService pregame;
    @Autowired GameStateRepository states;
    @Autowired RoomCleanupService cleanup;
    @Autowired SessionManager sessions;
    @Autowired ObjectMapper mapper;
    private final List<Peer> peers = new ArrayList<>();
    private final AtomicReference<Connection> accepted = new AtomicReference<>();
    private final AtomicReference<Peer> handshaking = new AtomicReference<>();
    private DisposableServer server;

    @AfterEach
    void tearDown() throws Exception {
        try {
            cleanup.cleanupRoom(ROOM).block(TIMEOUT);
            assertNull(states.findById(ROOM).block(TIMEOUT));
            assertTrue(sessions.getAllUser(ROOM).isEmpty());
        } finally {
            for (Peer peer : peers) {
                peer.socket.close();
                if (peer.connection != null) {
                    peer.connection.dispose();
                    peer.connection.onDispose().block(TIMEOUT);
                }
                if (peer.session != null) peer.ended.asMono().block(TIMEOUT);
            }
            if (server != null) server.disposeNow(TIMEOUT);
        }
    }

    @ParameterizedTest(name = "default buffers, pauseReading={0}")
    @ValueSource(booleans = {false, true})
    void fixedGameMeasuresTurnTrafficWithDefaultSocketBuffers(boolean pauseReading) throws Exception {
        assertTrue(AopUtils.isAopProxy(gameplay));
        startServer();
        Peer target = connect();
        Peer partner = connect();
        // 정상 분배 직후 상태만 고정한다. 이후 phase·카드는 실제 게임 프록시가 변경한다.
        states.create(GameState.builder().roomId(ROOM).leadingPlayer(1).currentTurn(1)
                .round(1).phase(GamePhase.IN_PROGRESS).build()).block(TIMEOUT);
        List<Card> deck = new ArrayList<>(Arrays.asList(Card.values()));
        Collections.shuffle(deck, new Random(20260731L));
        pregame.distributeCards(ROOM, deck).block(TIMEOUT);
        sessions.addPlayer(ROOM, Player.PLAYER_1, 101L, target.session).block(TIMEOUT);
        sessions.addPlayer(ROOM, Player.PLAYER_2, 102L, partner.session).block(TIMEOUT);
        // 송신량 측정은 대기 없이 순차 실행한다. 실제 타이머 발사·WS 요청 수신 실험은 아니다.
        TurnScheduler scheduler = mock(TurnScheduler.class);
        Map<String, List<String>> turnPayloads = new LinkedHashMap<>();
        Map<String, List<String>> partnerTurnPayloads = new LinkedHashMap<>();
        List<List<String>> targetBatches = new ArrayList<>();
        Map<GamePhase, Integer> commands = new EnumMap<>(GamePhase.class);
        long began = System.nanoTime();
        for (int i = 0; i < 80; i++) {
            assertTrue(System.nanoTime() - began < Duration.ofSeconds(20).toNanos(), "게임 측정 시간 상한");
            GameState state = states.findById(ROOM).block(TIMEOUT);
            assertNotNull(state);
            if (state.getPhase() == GamePhase.NONE) break;
            int firstBefore = target.outbound.size();
            int secondBefore = partner.outbound.size();
            Player actor = state.getCurrentPlayer();
            Mono<Void> action = switch (state.getPhase()) {
                case IN_PROGRESS -> turns.processNormalSubmit(ROOM, actor, 0, GameActionSource.USER, scheduler);
                case AWAITING_FLOOR_CARD_CHOICE -> turns.processFloorSelection(ROOM,
                        state.getChoiceInfo().getPlayerNumToChoose(), 0, GameActionSource.USER, scheduler);
                case AWAITING_GO_STOP_CHOICE -> turns.processGoStopChoice(ROOM, actor,
                        state.getPlayerState(actor).getGo() == 0, GameActionSource.USER, scheduler);
                default -> throw new AssertionError("Unexpected phase: " + state.getPhase());
            };
            action.block(TIMEOUT);
            commands.merge(state.getPhase(), 1, Integer::sum);
            List<String> first = List.copyOf(target.outbound.subList(firstBefore, target.outbound.size()));
            List<String> second = List.copyOf(partner.outbound.subList(secondBefore, partner.outbound.size()));
            assertFalse(first.isEmpty() && second.isEmpty());
            targetBatches.add(first);
            turnPayloads.computeIfAbsent(state.getRound() + ":" + state.getCurrentTurn(), k -> new ArrayList<>())
                    .addAll(first);
            partnerTurnPayloads.computeIfAbsent(state.getRound() + ":" + state.getCurrentTurn(), k -> new ArrayList<>())
                    .addAll(second);
            drainBatch(partner, second);
            if (!pauseReading) drainBatch(target, first);
        }
        assertEquals(GamePhase.NONE, states.findById(ROOM).block(TIMEOUT).getPhase(), "종료 후 재시작까지 완주");
        assertTrue(commands.getOrDefault(GamePhase.IN_PROGRESS, 0) >= 5);
        assertEquals(1, target.outbound.stream().filter(s -> status(s).equals("GAME_OVER")).count());
        assertEquals(1, partner.outbound.stream().filter(s -> status(s).equals("GAME_OVER")).count());
        TransportState beforeDrain = target.state();
        assertTrue(beforeDrain.active());
        if (pauseReading) {
            assertEquals(0, target.received, "handshake 이후 전체 게임 동안 애플리케이션 읽기 중단");
            for (List<String> batch : targetBatches) drainBatch(target, batch);
        }
        assertEquals(target.outbound.size(), target.received);
        assertEquals(partner.outbound.size(), partner.received);
        assertEquals(0, target.state().pendingBytes());
        assertEquals(0, partner.state().pendingBytes());
        Map<String, IntSummaryStatistics> events = new TreeMap<>();
        for (Peer peer : peers) {
            for (String payload : peer.outbound) {
                assertNotEquals("ERROR", status(payload));
                events.computeIfAbsent(status(payload), k -> new IntSummaryStatistics()).accept(bytes(payload));
            }
        }
        var config = target.connection.channel().config();
        System.out.printf("gameTraffic pause=%s commands=%s turns=%d targetFrames=%d targetBytes=%d "
                        + "partnerFrames=%d partnerBytes=%d targetMaxTurnFrames=%d targetMaxTurnBytes=%d partnerMaxTurnFrames=%d partnerMaxTurnBytes=%d "
                        + "serverSndbuf=%s peerRcvbuf=%d watermark=%s beforeDrain=%s afterDrain=%s events=%s%n",
                pauseReading, commands, turnPayloads.size(), target.outbound.size(), totalBytes(target.outbound),
                partner.outbound.size(), totalBytes(partner.outbound),
                turnPayloads.values().stream().mapToInt(List::size).max().orElseThrow(),
                turnPayloads.values().stream().mapToInt(GameTrafficSocketTest::totalBytes).max().orElseThrow(),
                partnerTurnPayloads.values().stream().mapToInt(List::size).max().orElseThrow(),
                partnerTurnPayloads.values().stream().mapToInt(GameTrafficSocketTest::totalBytes).max().orElseThrow(),
                config.getOption(io.netty.channel.ChannelOption.SO_SNDBUF), target.socket.getReceiveBufferSize(),
                config.getWriteBufferWaterMark(), beforeDrain, target.state(), events);
    }

    private String status(String payload) {
        try {
            return mapper.readTree(payload).get("status").asText();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static int bytes(String payload) {
        return payload.getBytes(StandardCharsets.UTF_8).length;
    }

    private static int totalBytes(List<String> payloads) {
        return payloads.stream().mapToInt(GameTrafficSocketTest::bytes).sum();
    }

    private void drainBatch(Peer peer, List<String> expected) throws Exception {
        List<String> actual = new ArrayList<>();
        for (int i = 0; i < expected.size(); i++) {
            actual.add(peer.read());
            peer.received++;
        }
        // 액션 내 Mono.when 안내끼리의 순서는 계약이 아니므로 액션별 본문·중복·누락을 비교한다.
        assertEquals(expected.stream().sorted().toList(), actual.stream().sorted().toList());
    }

    private void startServer() {
        HandshakeWebSocketService upgrade = new HandshakeWebSocketService();
        var handler = WebHttpHandlerBuilder.webHandler(exchange -> upgrade.handleRequest(exchange, session -> {
            Peer peer = handshaking.get();
            WebSocketSession observed = mock(WebSocketSession.class,
                    org.mockito.AdditionalAnswers.delegatesTo(session));
            doAnswer(invocation -> {
                Publisher<WebSocketMessage> messages = invocation.getArgument(0);
                return session.send(Flux.from(messages).doOnNext(message ->
                        peer.outbound.add(message.getPayloadAsText())));
            }).when(observed).send(any());
            peer.ready.tryEmitValue(observed);
            return session.receive().then().doFinally(signal -> peer.ended.tryEmitValue(true));
        })).build();
        server = HttpServer.create().host("127.0.0.1").port(0)
                .doOnConnection(accepted::set)
                .handle(new ReactorHttpHandlerAdapter(handler)).bindNow(TIMEOUT);
    }

    private Peer connect() throws Exception {
        Peer peer = new Peer();
        peers.add(peer);
        handshaking.set(peer);
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
        final List<String> outbound = new CopyOnWriteArrayList<>();
        int received;
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
            assertTrue(length <= 65535);
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
