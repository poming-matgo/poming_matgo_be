package com.pomingmatgo.gameservice.application.game;

import com.pomingmatgo.gameservice.domain.rule.ProcessCardResult;
import com.pomingmatgo.gameservice.domain.rule.SpecialEvent;

import com.pomingmatgo.gameservice.infrastructure.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.score.PayoutCalculator;
import com.pomingmatgo.gameservice.infrastructure.scheduler.TurnScheduler;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import com.pomingmatgo.gameservice.infrastructure.session.ActionNotificationOrder;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.util.context.Context;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.concurrent.atomic.AtomicReference;

import static com.pomingmatgo.gameservice.domain.TurnTiming.TURN_TIMEOUT_MILLIS;
import static com.pomingmatgo.gameservice.domain.TurnTiming.nextDeadlineNanos;

// 사용자 요청(WsGameHandler)과 자동플레이(AutoPlayScheduler)가 후처리를 공유해야 두 경로의 동작이 갈라지지 않는다.
// 상태 저장·타이머·END 재시작은 @GameLock 안에서 완료하고, 송신은 호출자의 구독에 남긴다.
// 타이머 조작은 필드가 아닌 TurnScheduler 파라미터로 주입 — DI cycle 회피
@Service
@RequiredArgsConstructor
public class TurnFlowService {

    private final RoomTimerLifecycle timerLifecycle;
    private final GamePlayService gamePlayService;
    private final GameMessageSender gameMessageSender;
    private final GameNotificationService gameNotificationService;
    private final PayoutCalculator payoutCalculator;
    private final SessionManager sessionManager;

    public Mono<Void> processNormalSubmit(long roomId, Player player, int cardIdx, GameActionSource source, TurnScheduler scheduler) {
        return withCompletion(roomId, source, scheduler,
                completion -> gamePlayService.executeNormalSubmit(roomId, player, cardIdx, completion),
                ctx -> {
                    Mono<Void> sendInfos = Mono.when(
                            gameMessageSender.sendSubmitCardInfo(roomId, player, ctx.submittedCard()),
                            gameMessageSender.sendTopCardInfo(roomId, player, ctx.topCard())
                    );

                    Mono<Void> handleResult = ctx.isChoiceRequired()
                            ? requestFloorChoice(roomId, player, ctx.cardResult().getSelectableCards())
                            : finishTurn(roomId, player, ctx.updatedGameState(), ctx.cardResult());

                    return sendInfos.then(handleResult);
                });
    }

    public Mono<Void> processFloorSelection(long roomId, Player player, int cardIdx, GameActionSource source, TurnScheduler scheduler) {
        return withCompletion(roomId, source, scheduler,
                completion -> gamePlayService.executeFloorSelection(roomId, player, cardIdx, completion),
                ctx -> ctx.isChoiceRequired()
                        // 뒤집은 카드가 또 선택을 요구한 경우 — 선택지 재전송 + 타이머 재등록
                        ? requestFloorChoice(roomId, player, ctx.cardResult().getSelectableCards())
                        : finishTurn(roomId, player, ctx.updatedGameState(), ctx.cardResult()));
    }

    public Mono<Void> processGoStopChoice(long roomId, Player player, boolean go, GameActionSource source, TurnScheduler scheduler) {
        return withCompletion(roomId, source, scheduler,
                completion -> gamePlayService.executeGoStop(roomId, player, go, completion),
                nextState -> {
                    if (nextState.isPlaying()) {
                        return gameMessageSender.sendGoResultMessage(nextState, player)
                                .then(gameMessageSender.sendTurnInfo(nextState, TURN_TIMEOUT_MILLIS));
                    }
                    return announceGameOver(nextState, player);
                });
    }

    // 구독마다 방 수명을 캡처하며 모든 액션이 같은 완료 정책을 반드시 전달한다.
    private <T> Mono<Void> withCompletion(long roomId, GameActionSource source, TurnScheduler scheduler,
                                       Function<GameActionCompletion, Mono<T>> action,
                                       Function<T, Mono<Void>> notification) {
        return Mono.defer(() -> {
            Objects.requireNonNull(source, "source");
            TurnScheduler bound = timerLifecycle.bind(roomId, Objects.requireNonNull(scheduler, "scheduler"));
            AtomicReference<UnaryOperator<Context>> recipients = new AtomicReference<>();
            AtomicReference<ActionNotificationOrder.Reservation> order = new AtomicReference<>();
            Disposable.Swap owned = Disposables.swap();
            GameActionCompletion completion = state -> Mono.defer(() -> {
                // 자동플레이는 이미 발사한 타이머를 별도로 취소하지 않는다.
                if (source == GameActionSource.USER) bound.cancelAutoPlay(roomId);
                scheduleNextStep(roomId, state, bound);
                Mono<Void> finish = state.getPhase() == GamePhase.END
                        ? gamePlayService.gameOver(state).then() : Mono.empty();
                // 후속 안내 대기 중 접속한 세션은 이미 이 액션을 포함한 스냅샷을 받는다.
                return finish.then(Mono.fromRunnable(() -> {
                    recipients.set(gameMessageSender.captureRecipients(roomId));
                    ActionNotificationOrder.Reservation reservation = sessionManager.reserveNotifications(roomId);
                    order.set(reservation);
                    // 수락 후 호출자가 먼저 취소됐어도 나중에 만든 예약이 남지 않아야 한다.
                    if (reservation != null) owned.update(reservation);
                }));
            });
            return action.apply(completion)
                    .flatMap(result -> {
                        ActionNotificationOrder.Reservation reservation = order.get();
                        Mono<Void> send = Mono.defer(() -> notification.apply(result)).contextWrite(recipients.get());
                        return reservation == null ? send : reservation.ready()
                                .flatMap(allowed -> allowed ? send : Mono.empty());
                    })
                    .doFinally(signal -> owned.dispose());
        });
    }

    /** 준비 흐름의 안내가 시작되기 전에 첫 턴 타이머를 등록한다. */
    public Mono<GameState> prepareFirstTurn(GameState state, TurnScheduler scheduler) {
        return Mono.fromSupplier(() -> {
            scheduleNextStep(state.getRoomId(), state, timerLifecycle.bind(state.getRoomId(), scheduler));
            return state;
        });
    }

    /** 정상 제출/바닥 선택 완료가 공유하는 턴 완료 처리 — 다음 단계는 이미 락 안에서 결정·저장돼 있다 */
    private Mono<Void> finishTurn(long roomId, Player player, GameState nextState, ProcessCardResult result) {
        return gameNotificationService.broadcastTurnResult(roomId, player, nextState, result)
                .then(notifyNextStep(nextState, player))
                .then();
    }

    private Mono<GameState> notifyNextStep(GameState nextState, Player player) {
        return switch (nextState.getPhase()) {
            // 스톱 판단엔 박 계열까지 반영된 정산이 필요하므로 승자를 본인으로 가정한 최종 정산을 싣는다
            case AWAITING_GO_STOP_CHOICE -> gameMessageSender.sendGoStopChoiceMessage(
                            nextState, player, payoutCalculator.finalPayout(nextState, player))
                    .thenReturn(nextState);
            case END -> announcePpeokWin(nextState, player)
                    .then(announceGameOver(nextState, endWinner(nextState, player)))
                    .thenReturn(nextState);
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

    public Mono<GameState> completeGameOver(GameState gameState) {
        return gamePlayService.gameOver(gameState);
    }

    public Mono<Void> announceGameOver(GameState finalState, Player winner) {
        return gameMessageSender.sendGameOverMessage(
                finalState, winner, payoutCalculator.finalPayout(finalState, winner));
    }

    // 선택 대기 타이머는 이미 락 내부에서 등록했다.
    private Mono<Void> requestFloorChoice(long roomId, Player player, List<Card> selectableCards) {
        return gameMessageSender.sendChooseFloorCardMessage(roomId, player, selectableCards);
    }

    // 대기 주체는 항상 currentPlayer — 고/스톱 대기면 방금 행동한 본인, 턴이 넘어갔으면 상대
    private void scheduleNextStep(long roomId, GameState nextState, TurnScheduler scheduler) {
        if (nextState.getPhase().isPlayerActionPhase()) {
            scheduler.scheduleAutoPlay(roomId, nextState.getRound(), nextState.getCurrentTurn(),
                    nextState.getCurrentPlayer(), nextDeadlineNanos(), nextState.getPhase());
        }
    }
}
