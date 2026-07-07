package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.response.websocket.GameMessageSender;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.scheduler.TurnScheduler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.pomingmatgo.gameservice.domain.GamePhase.AWAITING_FLOOR_CARD_CHOICE;
import static com.pomingmatgo.gameservice.domain.GamePhase.IN_PROGRESS;
import static com.pomingmatgo.gameservice.domain.TurnTiming.TURN_TIMEOUT_MILLIS;
import static com.pomingmatgo.gameservice.domain.TurnTiming.nextDeadlineNanos;

/**
 * 카드 제출/바닥 카드 선택의 실행 + 후처리(메시지 전송, 턴 진행, 자동플레이 타이머 재등록).
 *
 * 사용자 요청(WsGameHandler)과 자동플레이(AutoPlayScheduler)가 이 흐름을 공유하므로
 * 두 경로의 후처리 동작(선택 타이머 등록 여부 등)이 갈라질 수 없다.
 * 타이머 조작은 TurnScheduler 파라미터로 주입받는다 — DI cycle 회피 (TurnScheduler javadoc 참고).
 */
@Service
@RequiredArgsConstructor
public class TurnFlowService {

    private final GamePlayService gamePlayService;
    private final GameMessageSender gameMessageSender;
    private final GameNotificationService gameNotificationService;

    public Mono<Void> processNormalSubmit(long roomId, GameState gameState, Player player, int cardIdx, Runnable onLockAcquired, TurnScheduler scheduler) {
        return gamePlayService.executeNormalSubmit(roomId, gameState, player, cardIdx, onLockAcquired)
                .flatMap(ctx -> {
                    Mono<Void> sendInfos = Mono.when(
                            gameMessageSender.sendSubmitCardInfo(roomId, player, ctx.submittedCard()),
                            gameMessageSender.sendTopCardInfo(roomId, player, ctx.topCard())
                    );

                    Mono<Void> handleResult;
                    if (ctx.isChoiceRequired()) {
                        handleResult = requestFloorChoice(roomId, gameState, player, ctx.cardResult().getSelectableCards(), scheduler);
                    } else {
                        handleResult = gameNotificationService.broadcastTurnResult(roomId, player, ctx.updatedGameState(), ctx.cardResult(), () -> scheduler.cancelAutoPlay(roomId), TURN_TIMEOUT_MILLIS)
                                .doOnNext(nextState -> scheduleNextTurnIfNeeded(roomId, nextState, player, scheduler))
                                .then();
                    }

                    return sendInfos.then(handleResult);
                }).then();
    }

    public Mono<Void> processFloorSelection(long roomId, GameState gameState, Player player, int cardIdx, Runnable onLockAcquired, TurnScheduler scheduler) {
        return gamePlayService.executeFloorSelection(roomId, gameState, player, cardIdx, onLockAcquired)
                .flatMap(ctx -> {
                    if (ctx.isChoiceRequired()) {
                        // 뒤집은 카드 처리 결과가 또 다른 선택을 요구한 경우: 선택지 재전송 + 선택 타이머 재등록
                        return requestFloorChoice(roomId, gameState, player, ctx.cardResult().getSelectableCards(), scheduler);
                    }

                    return gameMessageSender.sendAcquiredCardMessage(roomId, player, ctx.cardResult().getAcquiredCards())
                            .then(gamePlayService.proceedToNextTurn(ctx.updatedGameState()))
                            .delayUntil(nextState -> gameNotificationService.broadcastNextTurnInfo(nextState, TURN_TIMEOUT_MILLIS))
                            .doOnNext(nextState -> scheduleNextTurnIfNeeded(roomId, nextState, player, scheduler))
                            .then();
                });
    }

    /**
     * 선택지를 전송하고 선택 단계 타이머를 등록한다.
     * 제한 시간 내 선택이 없으면 자동플레이가 대신 선택하므로 choice phase에서 게임이 멈추지 않는다.
     * 같은 (round, turn) 시퀀스라 기존 카드 제출 타이머는 원자적으로 교체된다.
     */
    private Mono<Void> requestFloorChoice(long roomId, GameState gameState, Player player, List<Card> selectableCards, TurnScheduler scheduler) {
        return gameMessageSender.sendChooseFloorCardMessage(roomId, player, selectableCards)
                .then(Mono.<Void>fromRunnable(() -> scheduler.scheduleAutoPlay(
                        roomId, gameState.getRound(), gameState.getCurrentTurn(), player, nextDeadlineNanos(), AWAITING_FLOOR_CARD_CHOICE)));
    }

    private void scheduleNextTurnIfNeeded(long roomId, GameState nextState, Player lastPlayer, TurnScheduler scheduler) {
        // currentPlayer가 그대로면 고/스톱 선택 대기 상태 → 카드 제출 타이머를 걸면 같은 턴 중복 제출로 이어진다
        if (nextState.getPhase() == IN_PROGRESS && !nextState.getCurrentPlayer().equals(lastPlayer)) {
            scheduler.scheduleAutoPlay(roomId, nextState.getRound(), nextState.getCurrentTurn(), nextState.getCurrentPlayer(), nextDeadlineNanos(), IN_PROGRESS);
        }
    }
}
