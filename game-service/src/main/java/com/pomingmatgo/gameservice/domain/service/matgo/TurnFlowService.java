package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.score.PayoutCalculator;
import com.pomingmatgo.gameservice.scheduler.TurnScheduler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.pomingmatgo.gameservice.domain.GamePhase.AWAITING_FLOOR_CARD_CHOICE;
import static com.pomingmatgo.gameservice.domain.TurnTiming.TURN_TIMEOUT_MILLIS;
import static com.pomingmatgo.gameservice.domain.TurnTiming.nextDeadlineNanos;

// 사용자 요청(WsGameHandler)과 자동플레이(AutoPlayScheduler)가 후처리를 공유해야 두 경로의 동작이 갈라지지 않는다.
// 상태 전이 저장은 GamePlayService가 @GameLock 안에서 끝내고 여기선 메시지/타이머만 다룬다.
// 타이머 조작은 필드가 아닌 TurnScheduler 파라미터로 주입 — DI cycle 회피
@Service
@RequiredArgsConstructor
public class TurnFlowService {

    private final GamePlayService gamePlayService;
    private final GameMessageSender gameMessageSender;
    private final GameNotificationService gameNotificationService;
    private final PayoutCalculator payoutCalculator;

    public Mono<Void> processNormalSubmit(long roomId, GameState gameState, Player player, int cardIdx, Runnable onLockAcquired, TurnScheduler scheduler) {
        return gamePlayService.executeNormalSubmit(roomId, gameState, player, cardIdx, onLockAcquired)
                .flatMap(ctx -> {
                    Mono<Void> sendInfos = Mono.when(
                            gameMessageSender.sendSubmitCardInfo(roomId, player, ctx.submittedCard()),
                            gameMessageSender.sendTopCardInfo(roomId, player, ctx.topCard())
                    );

                    Mono<Void> handleResult = ctx.isChoiceRequired()
                            ? requestFloorChoice(roomId, gameState, player, ctx.cardResult().getSelectableCards(), scheduler)
                            : finishTurn(roomId, player, ctx.updatedGameState(), ctx.cardResult(), scheduler);

                    return sendInfos.then(handleResult);
                }).then();
    }

    public Mono<Void> processFloorSelection(long roomId, GameState gameState, Player player, int cardIdx, Runnable onLockAcquired, TurnScheduler scheduler) {
        return gamePlayService.executeFloorSelection(roomId, gameState, player, cardIdx, onLockAcquired)
                .flatMap(ctx -> ctx.isChoiceRequired()
                        // 뒤집은 카드가 또 선택을 요구한 경우 — 선택지 재전송 + 타이머 재등록
                        ? requestFloorChoice(roomId, gameState, player, ctx.cardResult().getSelectableCards(), scheduler)
                        : finishTurn(roomId, player, ctx.updatedGameState(), ctx.cardResult(), scheduler));
    }

    public Mono<Void> processGoStopChoice(long roomId, GameState gameState, Player player, boolean go, Runnable onLockAcquired, TurnScheduler scheduler) {
        return gamePlayService.executeGoStop(roomId, gameState, player, go, onLockAcquired)
                .flatMap(nextState -> {
                    if (nextState.isPlaying()) {
                        return gameMessageSender.sendGoResultMessage(nextState, player)
                                .then(startTurn(nextState, scheduler));
                    }
                    return processGameOver(nextState, player).then();
                });
    }

    /** 첫 턴 시작(PreGameFlowService)이 이후 턴 전환과 같은 경로를 타게 하는 공개 진입점 */
    public Mono<Void> startTurn(GameState state, TurnScheduler scheduler) {
        return gameMessageSender.sendTurnInfo(state, TURN_TIMEOUT_MILLIS)
                .then(Mono.fromRunnable(() -> scheduleNextStep(state.getRoomId(), state, scheduler)));
    }

    /** 정상 제출/바닥 선택 완료가 공유하는 턴 완료 처리 — 다음 단계는 이미 락 안에서 결정·저장돼 있다 */
    private Mono<Void> finishTurn(long roomId, Player player, GameState nextState, ProcessCardResult result, TurnScheduler scheduler) {
        return gameNotificationService.broadcastTurnResult(roomId, player, nextState, result)
                .then(notifyNextStep(nextState, player))
                .doOnNext(finalState -> scheduleNextStep(roomId, finalState, scheduler))
                .then();
    }

    private Mono<GameState> notifyNextStep(GameState nextState, Player player) {
        return switch (nextState.getPhase()) {
            // 스톱 판단엔 박 계열까지 반영된 정산이 필요하므로 승자를 본인으로 가정한 최종 정산을 싣는다
            case AWAITING_GO_STOP_CHOICE -> gameMessageSender.sendGoStopChoiceMessage(
                            nextState, player, payoutCalculator.finalPayout(nextState, player))
                    .thenReturn(nextState);
            case END -> announcePpeokWin(nextState, player)
                    .then(processGameOver(nextState, endWinner(nextState, player)));
            default -> gameMessageSender.sendTurnInfo(nextState, TURN_TIMEOUT_MILLIS)
                    .thenReturn(nextState);
        };
    }

    // 종료 사유는 GAME_OVER 정산만으로 알 수 없어 세번뻑 승리를 먼저 알린다
    private Mono<Void> announcePpeokWin(GameState endedState, Player actor) {
        if (!endedState.hasPpeokWin(actor)) {
            return Mono.empty();
        }
        return gameMessageSender.sendSpecialEventMessageIfNeeded(
                endedState.getRoomId(), actor, SpecialEvent.THREE_PPEOK);
    }

    /** 세번뻑 즉시 승리 > 점수 달성자(최종 라운드 자동 스톱) > 마지막 턴 미달성 무승부 */
    private Player endWinner(GameState endedState, Player actor) {
        return endedState.hasPpeokWin(actor) || endedState.canGoStop(actor) ? actor : Player.PLAYER_NOTHING;
    }

    /** winner가 PLAYER_NOTHING이면 무승부 — 첫 턴 시작 전 종료(PreGameFlowService)도 이 경로를 공유한다 */
    public Mono<GameState> processGameOver(GameState gameState, Player winner) {
        return gamePlayService.gameOver(gameState)
                .delayUntil(finalState -> gameMessageSender.sendGameOverMessage(
                        finalState, winner, payoutCalculator.finalPayout(finalState, winner)));
    }

    // 타이머를 함께 걸어야 선택 대기 phase에서 게임이 멈추지 않는다
    private Mono<Void> requestFloorChoice(long roomId, GameState gameState, Player player, List<Card> selectableCards, TurnScheduler scheduler) {
        return gameMessageSender.sendChooseFloorCardMessage(roomId, player, selectableCards)
                .then(Mono.<Void>fromRunnable(() -> scheduler.scheduleAutoPlay(
                        roomId, gameState.getRound(), gameState.getCurrentTurn(), player, nextDeadlineNanos(), AWAITING_FLOOR_CARD_CHOICE)));
    }

    // 대기 주체는 항상 currentPlayer — 고/스톱 대기면 방금 행동한 본인, 턴이 넘어갔으면 상대
    private void scheduleNextStep(long roomId, GameState nextState, TurnScheduler scheduler) {
        if (nextState.getPhase().isPlayerActionPhase()) {
            scheduler.scheduleAutoPlay(roomId, nextState.getRound(), nextState.getCurrentTurn(),
                    nextState.getCurrentPlayer(), nextDeadlineNanos(), nextState.getPhase());
        }
    }
}
