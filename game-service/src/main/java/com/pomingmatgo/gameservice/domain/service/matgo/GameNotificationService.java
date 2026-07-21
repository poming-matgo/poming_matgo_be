package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.domain.messaging.ScoreInfoRes;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 턴 실행 결과 브로드캐스트(피 이동/획득/특수 이벤트/점수) — 순수 전송만 담당한다.
 * 다음 단계(게임 종료/고스톱 대기/다음 턴) 결정은 TurnFlowService.finishTurn의 책임.
 */
@Service
@RequiredArgsConstructor
public class GameNotificationService {
    private final GameMessageSender gameMessageSender;

    public Mono<Void> broadcastTurnResult(long roomId, Player player, GameState gameState, ProcessCardResult result) {
        Mono<Void> sendMoveCards = Flux.fromIterable(result.getMoveCards())
                .concatMap(card -> gameMessageSender.sendMovingCardMessage(roomId, player, gameState.getOtherPlayer(), card))
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
                .then(gameMessageSender.sendScoreInfo(gameState.getRoomId(), ScoreInfoRes.from(gameState)));
    }
}
