package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.domain.messaging.ScoreInfoRes;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.score.PayoutCalculator;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// 턴 결과 브로드캐스트만 담당 — 다음 단계 결정은 GamePlayService, 후속 메시지/타이머는 TurnFlowService
@Service
@RequiredArgsConstructor
public class GameNotificationService {
    private final GameMessageSender gameMessageSender;
    private final PayoutCalculator payoutCalculator;

    public Mono<Void> broadcastTurnResult(long roomId, Player player, GameState gameState, ProcessCardResult result) {
        // gameState는 턴 전환이 반영된 다음 상태일 수 있으므로 피를 잃는 쪽은 행위자(player) 기준으로 계산
        Mono<Void> sendMoveCards = Flux.fromIterable(result.getMoveCards())
                .concatMap(card -> gameMessageSender.sendMovingCardMessage(roomId, player, player.opponent(), card))
                .then();

        // 뻑은 획득 없이 바닥에 쌓이므로 획득 메시지를 보내지 않는다
        Mono<Void> sendAcquired = result.getSpecialEvents().contains(SpecialEvent.PPEOK)
                ? Mono.empty()
                : gameMessageSender.sendAcquiredCardMessage(roomId, player, result.getAcquiredCards());

        Mono<Void> sendSpecial = Flux.fromIterable(result.getSpecialEvents())
                .concatMap(event -> gameMessageSender.sendSpecialEventMessageIfNeeded(roomId, player, event))
                .then();

        return sendMoveCards
                .then(sendAcquired)
                .then(sendSpecial)
                .then(gameMessageSender.sendScoreInfo(gameState.getRoomId(), ScoreInfoRes.from(
                        gameState,
                        payoutCalculator.provisionalPayout(gameState, Player.PLAYER_1),
                        payoutCalculator.provisionalPayout(gameState, Player.PLAYER_2))));
    }
}
