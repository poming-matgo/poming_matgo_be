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

/**
 * 카드 제출/바닥 카드 선택/고스톱 선택의 실행 + 후처리(메시지 전송, 자동플레이 타이머 재등록).
 * 게임 상태 전이(턴 전환/고스톱 대기/종료 저장)는 GamePlayService가 @GameLock 안에서 끝내고,
 * 여기서는 저장된 다음 상태를 근거로 메시지와 타이머만 처리한다.
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
                        // 뒤집은 카드 처리 결과가 또 다른 선택을 요구한 경우: 선택지 재전송 + 선택 타이머 재등록
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

    /**
     * 턴 정보 공지 + 도달한 phase의 자동플레이 타이머 등록.
     * 첫 턴 시작(PreGameFlowService)이 이후 턴 전환과 같은 경로를 타게 하는 공개 진입점 —
     * 핸들러가 공지/타이머 등록을 직접 작성하면 유저/자동 경로의 동작이 갈라진다.
     */
    public Mono<Void> startTurn(GameState state, TurnScheduler scheduler) {
        return gameMessageSender.sendTurnInfo(state, TURN_TIMEOUT_MILLIS)
                .then(Mono.fromRunnable(() -> scheduleNextStep(state.getRoomId(), state, scheduler)));
    }

    /**
     * 턴 실행 결과의 공통 완료 처리: 결과 브로드캐스트(GameNotificationService) 후,
     * 락 안에서 이미 저장된 다음 단계(게임 종료/고스톱 대기/다음 턴)에 맞는 메시지를 보내고
     * 자동플레이 타이머를 등록한다. 다음 단계의 결정·저장은 GamePlayService가 @GameLock 안에서 끝냈다.
     * 정상 제출/바닥 선택 완료가 모두 이 흐름을 거치므로 두 경로의 후처리 동작이 갈라질 수 없다.
     */
    private Mono<Void> finishTurn(long roomId, Player player, GameState nextState, ProcessCardResult result, TurnScheduler scheduler) {
        return gameNotificationService.broadcastTurnResult(roomId, player, nextState, result)
                .then(notifyNextStep(nextState, player))
                .doOnNext(finalState -> scheduleNextStep(roomId, finalState, scheduler))
                .then();
    }

    /** 도달한 phase별 후처리 메시지: 고/스톱 선택지 전송, 게임 종료 정리(END는 승자/무승부 판정 포함), 다음 턴 공지 */
    private Mono<GameState> notifyNextStep(GameState nextState, Player player) {
        return switch (nextState.getPhase()) {
            case AWAITING_GO_STOP_CHOICE -> gameMessageSender.sendGoStopChoiceMessage(nextState, player)
                    .thenReturn(nextState);
            // 점수 달성자가 있으면 그 승리(최종 라운드 자동 스톱), 없으면 마지막 턴 미달성 무승부
            case END -> processGameOver(nextState, nextState.canGoStop(player) ? player : Player.PLAYER_NOTHING);
            default -> gameMessageSender.sendTurnInfo(nextState, TURN_TIMEOUT_MILLIS)
                    .thenReturn(nextState);
        };
    }

    /** winner가 PLAYER_NOTHING이면 무승부 */
    private Mono<GameState> processGameOver(GameState gameState, Player winner) {
        return gamePlayService.gameOver(gameState)
                .delayUntil(finalState -> gameMessageSender.sendGameOverMessage(
                        finalState, winner, payoutCalculator.finalPayout(finalState, winner)));
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

    /**
     * 도달한 phase가 행동 대기면 그 phase의 자동플레이 타이머를 등록한다.
     * 대기 주체는 항상 currentPlayer — 고/스톱 대기는 (round, turn)이 유지되므로 방금 행동한 플레이어 자신이고,
     * 다음 턴 진행이면 턴이 넘어간 상대다. 같은 턴의 낡은 타이머는 TurnStep 순서 비교로 원자적으로 교체된다.
     */
    private void scheduleNextStep(long roomId, GameState nextState, TurnScheduler scheduler) {
        if (nextState.getPhase().isPlayerActionPhase()) {
            scheduler.scheduleAutoPlay(roomId, nextState.getRound(), nextState.getCurrentTurn(),
                    nextState.getCurrentPlayer(), nextDeadlineNanos(), nextState.getPhase());
        }
    }
}
