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
import static com.pomingmatgo.gameservice.domain.TurnTiming.TURN_TIMEOUT_MILLIS;
import static com.pomingmatgo.gameservice.domain.TurnTiming.nextDeadlineNanos;

/**
 * 카드 제출/바닥 카드 선택/고스톱 선택의 실행 + 후처리(메시지 전송, 턴 진행, 자동플레이 타이머 재등록).
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
                                .then(gameMessageSender.sendTurnInfo(nextState, TURN_TIMEOUT_MILLIS))
                                .then(Mono.fromRunnable(() -> scheduleNextStep(roomId, nextState, scheduler)));
                    }
                    return gamePlayService.gameOver(nextState, player)
                            .delayUntil(finalState -> gameMessageSender.sendGameOverMessage(finalState, player))
                            .then();
                });
    }

    /**
     * 턴 실행 결과의 공통 완료 처리: 결과 브로드캐스트 후 다음 단계(게임 종료/고스톱 대기/다음 턴) 결정은
     * GameNotificationService에 위임하고, 도달한 phase에 맞는 자동플레이 타이머를 등록한다.
     * 정상 제출/바닥 선택 완료가 모두 이 흐름을 거치므로 바닥 선택으로 끝난 턴에도
     * 마지막 턴 판정과 고/스톱 선택 기회가 동일하게 적용된다.
     */
    private Mono<Void> finishTurn(long roomId, Player player, GameState updatedState, ProcessCardResult result, TurnScheduler scheduler) {
        return gameNotificationService.broadcastTurnResult(roomId, player, updatedState, result, TURN_TIMEOUT_MILLIS)
                .doOnNext(nextState -> scheduleNextStep(roomId, nextState, scheduler))
                .then();
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
