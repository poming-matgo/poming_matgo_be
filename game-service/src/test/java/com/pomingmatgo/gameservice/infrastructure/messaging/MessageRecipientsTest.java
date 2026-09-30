package com.pomingmatgo.gameservice.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import com.pomingmatgo.gameservice.infrastructure.session.SnapshotDelivery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void pendingSnapshotWaitEndsWhenRegistrationIsRemoved(boolean removeRoom) {
        WebSocketSession original = session("pending");
        SnapshotDelivery delivery = new SnapshotDelivery();
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, original, delivery).block(TIMEOUT);
        delivery.captured();
        var recipients = sender.captureRecipients(ROOM_ID);
        StepVerifier.create(sender.sendPayload(original, "old-action").contextWrite(recipients))
                .then(() -> {
                    verify(original, never()).send(any());
                    if (removeRoom) sessions.removeRoom(ROOM_ID).block(TIMEOUT);
                    else sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, original).block(TIMEOUT);
                })
                .verifyComplete();
        delivery.complete(true);
        sender.sendPayload(original, "late-action").contextWrite(recipients).block(TIMEOUT);
        verify(original, never()).send(any());
        sessions.shutdown();
    }

    @Test
    void sameSocketReregistrationDoesNotRestoreOldRecipients() {
        WebSocketSession original = session("same-socket");
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, original).block(TIMEOUT);
        var recipients = sender.captureRecipients(ROOM_ID);
        sessions.deletePlayer(ROOM_ID, 1, original);
        sessions.addPlayer(ROOM_ID, Player.PLAYER_1, 1L, original).block(TIMEOUT);
        sender.sendPayload(original, "old-action").contextWrite(recipients).block(TIMEOUT);
        verify(original, never()).send(any());
        sender.sendPayload(original, "new-action").contextWrite(sender.captureRecipients(ROOM_ID)).block(TIMEOUT);
        verify(original).send(any());
        sessions.shutdown();
    }

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
