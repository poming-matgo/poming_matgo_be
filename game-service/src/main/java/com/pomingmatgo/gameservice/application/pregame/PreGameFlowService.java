package com.pomingmatgo.gameservice.application.pregame;

import com.pomingmatgo.gameservice.application.game.TurnFlowService;
import com.pomingmatgo.gameservice.application.pregame.PreGameStartService.PreparedStart;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.infrastructure.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import com.pomingmatgo.gameservice.infrastructure.messaging.ActionNotificationScope;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_NOTHING;
import static com.pomingmatgo.gameservice.domain.TurnTiming.TURN_TIMEOUT_MILLIS;

@Service
@RequiredArgsConstructor
public class PreGameFlowService {
    private final PreGameStartService startService;
    private final GameMessageSender gameMessageSender;
    private final TurnFlowService turnFlowService;
    private final SessionManager sessionManager;

    public Mono<Void> processLeaderSelection(GameState gameState, Player player, int cardIndex) {
        long roomId = gameState.getRoomId();
        return Mono.defer(() -> {
            ActionNotificationScope notifications = new ActionNotificationScope(roomId, sessionManager,
                    () -> gameMessageSender.captureRecipients(roomId));
            return startService.selectAndPrepare(roomId, player, cardIndex, notifications::capture)
                    .map(Optional::of)
                    .defaultIfEmpty(Optional.empty())
                    .flatMap(start -> notifications.send(() ->
                            gameMessageSender.sendLeaderSelectionMessage(roomId, player, cardIndex)
                                    .then(Mono.defer(() -> start.isPresent()
                                            ? notifyGameStart(roomId, start.get()) : Mono.empty()))))
                    .doFinally(signal -> notifications.dispose());
        });
    }

    private Mono<Void> notifyGameStart(long roomId, PreparedStart start) {
        return Mono.defer(() -> gameMessageSender.sendLeaderSelectionResult(roomId, start.result()))
                .then(Mono.defer(() -> gameMessageSender.sendDistributedCardInfo(roomId, start.cards())))
                .then(Mono.defer(() -> start.draw()
                        ? turnFlowService.announceGameOver(start.state(), PLAYER_NOTHING)
                        : gameMessageSender.sendTurnInfo(start.state(), TURN_TIMEOUT_MILLIS)));
    }

}
