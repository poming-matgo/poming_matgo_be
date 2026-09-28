package com.pomingmatgo.gameservice.application.pregame;

import com.pomingmatgo.gameservice.application.game.TurnFlowService;
import com.pomingmatgo.gameservice.application.pregame.PreGameStartService.PreparedStart;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.infrastructure.messaging.GameMessageSender;
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

    public Mono<Void> processLeaderSelection(GameState gameState, Player player, int cardIndex) {
        long roomId = gameState.getRoomId();
        // 필수 처리는 별도 빈의 프록시를 통과하고, 안내 구독은 연결이 소유한다.
        return startService.selectAndPrepare(roomId, player, cardIndex)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .flatMap(start -> Mono.defer(() -> gameMessageSender.sendLeaderSelectionMessage(roomId, player, cardIndex))
                        .then(Mono.defer(() -> start.isPresent() ? notifyGameStart(roomId, start.get()) : Mono.empty())));
    }

    private Mono<Void> notifyGameStart(long roomId, PreparedStart start) {
        return Mono.defer(() -> gameMessageSender.sendLeaderSelectionResult(roomId, start.result()))
                .then(Mono.defer(() -> gameMessageSender.sendDistributedCardInfo(roomId, start.cards())))
                .then(Mono.defer(() -> start.draw()
                        ? turnFlowService.announceGameOver(start.state(), PLAYER_NOTHING)
                        : gameMessageSender.sendTurnInfo(start.state(), TURN_TIMEOUT_MILLIS)));
    }

}
