package com.pomingmatgo.gameservice.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MessageRecipientsTest {
    private static final long ROOM_ID = 80L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final SessionManager sessions = new SessionManager();
    private final MessageSender sender = new MessageSender(new ObjectMapper(), sessions,
            new StaticListableBeanFactory().getBeanProvider(ThroughputRecorder.class));
    private final GameMessageSender gameSender = new GameMessageSender(sender, sessions);

    @Test
    void targetedAndBroadcastMessagesShareRecipientsWithoutLeakingIntoLaterActions() {
        WebSocketSession original = session("original");
        WebSocketSession replacement = session("replacement");
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, original).block(TIMEOUT);
        var recipients = sender.captureRecipients(ROOM_ID);
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, replacement).block(TIMEOUT);
        try {
            gameSender.sendChooseFloorCardMessage(ROOM_ID, Player.PLAYER_1, List.of(Card.JAN_1))
                    .then(gameSender.sendSubmitCardInfo(ROOM_ID, Player.PLAYER_1, Card.JAN_1))
                    .contextWrite(recipients).block(TIMEOUT);
            verify(original, never()).send(any());
            verify(replacement, never()).send(any());

            gameSender.sendChooseFloorCardMessage(ROOM_ID, Player.PLAYER_1, List.of(Card.JAN_1))
                    .contextWrite(sender.captureRecipients(ROOM_ID)).block(TIMEOUT);
            gameSender.sendSubmitCardInfo(ROOM_ID, Player.PLAYER_1, Card.JAN_1).block(TIMEOUT);
            verify(replacement, times(2)).send(any());
        } finally {
            sessions.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cleanupExcludesCapturedOpenSessionAndNewRoomRecipients(boolean reuseSession) {
        WebSocketSession original = session("old-room");
        WebSocketSession replacement = reuseSession ? original : session("new-room");
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, original).block(TIMEOUT);
        var recipients = sender.captureRecipients(ROOM_ID);
        Mono<Void> targeted = gameSender.sendChooseFloorCardMessage(ROOM_ID, Player.PLAYER_1, List.of(Card.JAN_1));
        sessions.removeRoom(ROOM_ID).block(TIMEOUT);
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, replacement).block(TIMEOUT);
        try {
            targeted.then(gameSender.sendSubmitCardInfo(ROOM_ID, Player.PLAYER_1, Card.JAN_1))
                    .contextWrite(recipients).block(TIMEOUT);
            verify(original, never()).send(any());
            verify(replacement, never()).send(any());
        } finally {
            sessions.shutdown();
        }
    }

    private WebSocketSession session(String id) {
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn(id);
        when(session.isOpen()).thenReturn(true);
        when(session.close()).thenReturn(Mono.empty());
        when(session.send(any())).thenReturn(Mono.empty());
        when(session.textMessage(anyString())).thenAnswer(invocation -> new WebSocketMessage(
                WebSocketMessage.Type.TEXT, DefaultDataBufferFactory.sharedInstance.wrap(
                        invocation.<String>getArgument(0).getBytes(StandardCharsets.UTF_8))));
        return session;
    }
}
