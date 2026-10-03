package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.application.game.*;
import com.pomingmatgo.gameservice.application.pregame.PreGameService;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.*;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.infrastructure.lock.InFlightManager;
import com.pomingmatgo.gameservice.infrastructure.scheduler.AutoPlayScheduler;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
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
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.socket.*;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import reactor.core.Disposable;
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
    private static final long OTHER_ROOM = 981_002L;
    @Autowired TurnFlowService turns;
    @Autowired GamePlayService gameplay;
    @Autowired PreGameService pregame;
    @Autowired GameStateRepository states;
    @Autowired RoomCleanupService cleanup;
    @Autowired SessionManager sessions;
    @Autowired ObjectMapper mapper;
    @Autowired GameWebSocketHandler socketHandler;
    @Autowired AutoPlayScheduler autoPlay;
    @Autowired RoomTimerLifecycle timerLifecycle;
    @Autowired InFlightManager inFlight;
    private final List<Peer> peers = new ArrayList<>();
    private final AtomicReference<Connection> accepted = new AtomicReference<>();
    private final AtomicReference<Peer> handshaking = new AtomicReference<>();
    private DisposableServer server;

    @AfterEach
    void tearDown() throws Exception {
        try {
            for (long room : List.of(ROOM, OTHER_ROOM)) {
                cleanup.cleanupRoom(room).block(TIMEOUT);
                assertNull(states.findById(room).block(TIMEOUT));
                assertTrue(sessions.getAllUser(room).isEmpty());
                assertFalse(scheduled().containsKey(room));
            }
        } finally {
            for (Peer peer : peers) {
                peer.socket.close();
                if (peer.connection != null) {
                    peer.connection.dispose();
                    peer.connection.onDispose().block(TIMEOUT);
                }
                if (peer.session != null) peer.ended.asMono().block(TIMEOUT);
                if (peer.connection != null) {
                    assertFalse(peer.state().active());
                    assertEquals(0, peer.state().pendingBytes());
                }
            }
            if (server != null) server.disposeNow(TIMEOUT);
            await(() -> runningAutoPlays().size() == 0 &&
                    ((Disposable.Composite) ReflectionTestUtils.getField(socketHandler, "pendingDisconnects")).size() == 0);
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

    @ParameterizedTest(name = "mixed WS/autoplay, pauseReading={0}")
    @ValueSource(booleans = {false, true})
    void twoRoomsProgressThroughWebSocketAndAutoplayWithDefaultBuffers(boolean pauseReading) throws Exception {
        startServer();
        Peer[] players = {connect(), connect(), connect(), connect()};
        for (int roomIndex = 0; roomIndex < 2; roomIndex++) {
            long room = roomIndex == 0 ? ROOM : OTHER_ROOM;
            states.create(GameState.builder().roomId(room).leadingPlayer(1).currentTurn(1)
                    .round(1).phase(GamePhase.IN_PROGRESS).build()).block(TIMEOUT);
            timerLifecycle.open(room);
            List<Card> deck = new ArrayList<>(Arrays.asList(Card.values()));
            Collections.shuffle(deck, new Random(20260731L));
            pregame.distributeCards(room, deck).block(TIMEOUT);
            sessions.addPlayer(room, Player.PLAYER_1, 101L, players[roomIndex * 2].session).block(TIMEOUT);
            sessions.addPlayer(room, Player.PLAYER_2, 102L, players[roomIndex * 2 + 1].session).block(TIMEOUT);
        }
        List<List<String>> pausedBatches = new ArrayList<>();
        int[] actions = new int[2];
        int automatic = 0;
        long began = System.nanoTime();
        for (int step = 0; step < 80; step++) {
            boolean finished = true;
            for (int roomIndex = 0; roomIndex < 2; roomIndex++) {
                long room = roomIndex == 0 ? ROOM : OTHER_ROOM;
                GameState state = states.findById(room).block(TIMEOUT);
                if (state.getPhase() == GamePhase.NONE) continue;
                finished = false;
                assertTrue(System.nanoTime() - began < Duration.ofSeconds(30).toNanos());
                Peer first = players[roomIndex * 2];
                Peer second = players[roomIndex * 2 + 1];
                int beforeFirst = first.outbound.size();
                int beforeSecond = second.outbound.size();
                Player actor = state.getCurrentPlayer();
                if (roomIndex == 0 && actor == Player.PLAYER_1) {
                    // 만료 시각만 앞당긴다. 발사·InFlight·게임 처리·다음 예약은 실제 스케줄러를 통과한다.
                    autoPlay.scheduleAutoPlay(room, state.getRound(), state.getCurrentTurn(), actor,
                            System.nanoTime(), state.getPhase());
                    automatic++;
                } else {
                    String event = switch (state.getPhase()) {
                        case IN_PROGRESS -> "NORMAL_SUBMIT";
                        case AWAITING_FLOOR_CARD_CHOICE -> "FLOOR_SELECT";
                        case AWAITING_GO_STOP_CHOICE -> "GO_STOP_CHOICE";
                        default -> throw new AssertionError(state.getPhase());
                    };
                    (actor == Player.PLAYER_1 ? first : second).write(
                            "{\"eventType\":{\"subType\":\"" + event + "\"},\"data\":{\"cardIndex\":0,\"go\":false}}");
                }
                await(() -> first.outbound.size() > beforeFirst
                        && runningAutoPlays().size() == 0
                        && !inFlight.isSet(InFlightManager.normalKey(room, actor.getNumber())).block(TIMEOUT));
                GameState next = states.findById(room).block(TIMEOUT);
                if (next.getPhase() != GamePhase.NONE) {
                    Object timer = scheduled().get(room);
                    assertNotNull(timer, "후속 행동 타이머 등록");
                    Object nextStep = ReflectionTestUtils.getField(timer, "step");
                    assertEquals(next.getRound(), ReflectionTestUtils.getField(nextStep, "round"));
                    assertEquals(next.getCurrentTurn(), ReflectionTestUtils.getField(nextStep, "turn"));
                    assertEquals(next.getPhase(), ReflectionTestUtils.getField(nextStep, "phase"));
                }
                List<String> batch = List.copyOf(first.outbound.subList(beforeFirst, first.outbound.size()));
                if (pauseReading && roomIndex == 0) pausedBatches.add(batch);
                else drainBatch(first, batch);
                drainBatch(second, List.copyOf(second.outbound.subList(beforeSecond, second.outbound.size())));
                actions[roomIndex]++;
            }
            if (finished) break;
        }
        for (long room : List.of(ROOM, OTHER_ROOM)) {
            assertEquals(GamePhase.NONE, states.findById(room).block(TIMEOUT).getPhase());
            assertFalse(scheduled().containsKey(room));
        }
        assertTrue(automatic > 0);
        assertTrue(actions[0] > 5 && actions[1] > 5);
        TransportState beforeDrain = players[0].state();
        if (pauseReading) {
            assertEquals(0, players[0].received);
            for (List<String> batch : pausedBatches) drainBatch(players[0], batch);
        }
        for (Peer peer : players) {
            assertEquals(peer.outbound.size(), peer.received);
            assertEquals(1, peer.outbound.stream().filter(p -> status(p).equals("GAME_OVER")).count());
            assertTrue(peer.outbound.stream().noneMatch(p -> status(p).equals("ERROR")));
            assertEquals(0, peer.state().pendingBytes());
        }
        assertEquals(0, runningAutoPlays().size());
        System.out.printf("mixedGame pause=%s actions=%s autoplay=%d frames=%s beforeDrain=%s%n",
                pauseReading, Arrays.toString(actions), automatic,
                peers.stream().map(p -> p.received).toList(), beforeDrain);
    }

    private Map<?, ?> scheduled() {
        return (Map<?, ?>) ReflectionTestUtils.getField(autoPlay, "scheduled");
    }

    private Disposable.Composite runningAutoPlays() {
        return (Disposable.Composite) ReflectionTestUtils.getField(autoPlay, "runningAutoPlays");
    }

    private void await(java.util.function.BooleanSupplier condition) {
        Mono.defer(() -> condition.getAsBoolean() ? Mono.just(true) : Mono.empty())
                .repeatWhen(repeat -> repeat.delayElements(Duration.ofMillis(10)))
                .next().block(TIMEOUT);
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
            return socketHandler.handle(observed).doFinally(signal -> peer.ended.tryEmitValue(true));
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

        void write(String payload) throws Exception {
            byte[] body = payload.getBytes(StandardCharsets.UTF_8);
            assertTrue(body.length < 126);
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            frame.write(0x81);
            frame.write(0x80 | body.length);
            byte[] mask = {17, 31, 47, 61};
            frame.write(mask);
            for (int i = 0; i < body.length; i++) frame.write(body[i] ^ mask[i % 4]);
            socket.getOutputStream().write(frame.toByteArray());
            socket.getOutputStream().flush();
        }

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
