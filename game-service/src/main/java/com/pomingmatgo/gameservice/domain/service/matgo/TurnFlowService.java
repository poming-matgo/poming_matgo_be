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
                    return processGameOver(nextState, player).then();
                });
    }

    /**
     * 턴 정보 공지 + 도달한 phase의 자동플레이 타이머 등록.
     * 첫 턴 시작(WsPreGameHandler)이 이후 턴 전환과 같은 경로를 타게 하는 공개 진입점 —
     * 핸들러가 공지/타이머 등록을 직접 작성하면 유저/자동 경로의 동작이 갈라진다.
     */
    public Mono<Void> startTurn(GameState state, TurnScheduler scheduler) {
        return gameMessageSender.sendTurnInfo(state, TURN_TIMEOUT_MILLIS)
                .then(Mono.fromRunnable(() -> scheduleNextStep(state.getRoomId(), state, scheduler)));
    }

    /**
     * 턴 실행 결과의 공통 완료 처리: 결과 브로드캐스트(GameNotificationService) 후 다음 단계
     * (게임 종료/고스톱 대기/다음 턴)를 결정하고, 도달한 phase에 맞는 자동플레이 타이머를 등록한다.
     * 정상 제출/바닥 선택 완료가 모두 이 흐름을 거치므로 바닥 선택으로 끝난 턴에도
     * 마지막 턴 판정과 고/스톱 선택 기회가 동일하게 적용된다.
     */
    private Mono<Void> finishTurn(long roomId, Player player, GameState updatedState, ProcessCardResult result, TurnScheduler scheduler) {
        return gameNotificationService.broadcastTurnResult(roomId, player, updatedState, result)
                .then(proceedAfterTurn(updatedState, player))
                .doOnNext(nextState -> scheduleNextStep(roomId, nextState, scheduler))
                .then();
    }

    /** 턴 완료 후 다음 단계 결정: 마지막 턴/최종 라운드의 점수 달성 → 게임 종료, 점수 달성 → 고/스톱 대기, 그 외 → 다음 턴 */
    private Mono<GameState> proceedAfterTurn(GameState gameState, Player player) {
        if (gameState.isLastTurn()) {
            return processGameOver(gameState, player);
        }
        if (gamePlayService.canGoStop(gameState, player)) {
            // 마지막 라운드엔 GO 선택지가 없으므로 곧바로 게임 종료
            if (gameState.isFinalRound()) {
                return processGameOver(gameState, player);
            }
            // phase를 저장해 두면 선택 요청 검증과 자동플레이 타이머가 이 상태를 근거로 동작한다.
            // 타이머 등록은 반환된 phase를 보고 finishTurn의 scheduleNextStep이 수행
            return gamePlayService.enterGoStopChoice(gameState)
                    .delayUntil(awaitingState -> gameMessageSender.sendGoStopChoiceMessage(awaitingState, player));
        }
        return gamePlayService.proceedToNextTurn(gameState)
                .flatMap(nextState -> gameMessageSender.sendTurnInfo(nextState, TURN_TIMEOUT_MILLIS)
                        .thenReturn(nextState));
    }

    private Mono<GameState> processGameOver(GameState gameState, Player player) {
        return gamePlayService.gameOver(gameState)
                .delayUntil(finalState -> gameMessageSender.sendGameOverMessage(finalState, player));
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
