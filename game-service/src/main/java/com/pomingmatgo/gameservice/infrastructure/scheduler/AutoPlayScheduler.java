package com.pomingmatgo.gameservice.infrastructure.scheduler;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.TurnTiming;
import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.application.game.GameService;
import com.pomingmatgo.gameservice.application.game.TurnFlowService;
import com.pomingmatgo.gameservice.application.game.GameActionSource;
import com.pomingmatgo.gameservice.infrastructure.lock.InFlightManager;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

// 타이머는 서버 인스턴스별로 관리하므로 같은 방의 요청은 같은 인스턴스로 라우팅해야 한다.
// 다른 인스턴스의 타이머는 취소할 수 없어 불필요한 자동플레이가 시작될 수 있다.
@Service
@RequiredArgsConstructor
@Log4j2
public class AutoPlayScheduler implements TurnScheduler {

    private static final int AUTO_PLAY_CARD_INDEX = 0;
    // 응답이 없는 플레이어는 승리를 확정하도록 GO 대신 STOP을 선택한다.
    private static final boolean AUTO_GO_STOP_IS_GO = false;
    private static final long MIN_DELAY_MILLIS = 100;

    private final RoomTimerLifecycle timerLifecycle;
    private final InFlightManager inFlightManager;
    private final GameService gameService;
    private final TurnFlowService turnFlowService;

    // 타이머 교체 여부를 비교하고 실행 시 게임 상태를 재검증하는 기준이다.
    // 같은 라운드와 턴에서는 GamePhase.turnStepOrder를 비교해 이전 단계의 타이머가 다음 단계의 타이머를 덮어쓰지 않게 한다.
    private record TurnStep(int round, int turn, GamePhase phase) implements Comparable<TurnStep> {

        @Override
        public int compareTo(TurnStep other) {
            int c = Integer.compare(this.round, other.round);
            if (c != 0) return c;
            c = Integer.compare(this.turn, other.turn);
            if (c != 0) return c;
            return Integer.compare(this.phase.getTurnStepOrder(), other.phase.getTurnStepOrder());
        }

        boolean matches(GameState gameState) {
            return gameState.getRound() == round
                    && gameState.getCurrentTurn() == turn
                    && gameState.getPhase() == phase;
        }
    }

    private record Scheduled(TurnStep step, long deadlineNanos, Disposable task) {}

    private final Map<Long, Scheduled> scheduled = new ConcurrentHashMap<>();
    private final Disposable.Composite runningAutoPlays = Disposables.composite();
    private boolean stopped;

    /** 재접속 화면에 표시할 남은 시간의 근사값으로, 타이머 마감 시각에 포함된 유예 시간을 제외한다. */
    @Override
    public long getRemainingTurnMillis(long roomId) {
        Scheduled current = scheduled.get(roomId);
        if (current == null) return TurnTiming.TURN_TIMEOUT_MILLIS;
        long remaining = TimeUnit.NANOSECONDS.toMillis(current.deadlineNanos() - System.nanoTime())
                - TurnTiming.GRACE_PERIOD_MILLIS;
        return Math.max(remaining, 0);
    }

    @Override
    public void scheduleAutoPlay(long roomId, int round, int currentTurn, Player currentPlayer, long deadlineNanos, GamePhase expectedPhase) {
        // 방·서버 종료와 타이머 등록을 직렬화한다. 흐름의 수명 검증도 같은 monitor를 사용한다.
        synchronized (timerLifecycle) {
            if (stopped || !timerLifecycle.isOpen(roomId)) return;
            registerAutoPlay(roomId, round, currentTurn, currentPlayer, deadlineNanos, expectedPhase);
        }
    }

    private void registerAutoPlay(long roomId, int round, int currentTurn, Player currentPlayer, long deadlineNanos, GamePhase expectedPhase) {
        TurnStep newStep = new TurnStep(round, currentTurn, expectedPhase);

        long delayMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (delayMillis <= 0) delayMillis = MIN_DELAY_MILLIS;

        // 대기 타이머 취소가 실행 중인 게임 처리와 GAME_OVER/GO_STOP_CHOICE 전송을 중단하지 않도록 구독을 분리한다.
        // 실행 중 요청 경합은 TurnStep 재검증, InFlight, @GameLock으로 제어한다.
        TurnScheduler boundScheduler = timerLifecycle.bind(roomId, this);
        Disposable newTask = Mono.delay(Duration.ofMillis(delayMillis))
                .subscribe(v -> startAutoPlay(roomId, newStep, currentPlayer, boundScheduler));

        // 기존 타이머가 더 나중 단계일 때만 유지한다. 연속 바닥 카드 선택을 위해 같은 단계의 타이머는 교체한다.
        Disposable[] toDispose = new Disposable[1];
        scheduled.compute(roomId, (k, prev) -> {
            if (prev != null && prev.step.compareTo(newStep) > 0) {
                toDispose[0] = newTask;
                return prev;
            }
            toDispose[0] = (prev != null) ? prev.task : null;
            return new Scheduled(newStep, deadlineNanos, newTask);
        });

        if (toDispose[0] != null && !toDispose[0].isDisposed()) {
            toDispose[0].dispose();
        }
    }

    private void startAutoPlay(long roomId, TurnStep step, Player currentPlayer, TurnScheduler boundScheduler) {
        BaseSubscriber<Void> execution = new BaseSubscriber<>() {
            @Override
            protected void hookOnError(Throwable error) {
                log.error("[AutoPlay] 룸({}) 자동플레이 실행 중 에러 발생!", roomId, error);
            }

            @Override
            protected void hookFinally(SignalType type) {
                runningAutoPlays.remove(this);
            }
        };
        // 구독 전에 등록해 즉시 완료된 구독이 남거나 서버 종료 시 회수에서 빠지는 일을 막는다.
        if (runningAutoPlays.add(execution)) {
            Scheduled fired = scheduled.get(roomId);
            Mono.defer(() -> attemptAutoPlay(roomId, step, currentPlayer, boundScheduler, fired)).subscribe(execution);
        }
    }

    @PreDestroy
    public void shutdown() {
        Scheduled[] pending;
        synchronized (timerLifecycle) {
            stopped = true;
            timerLifecycle.clear();
            pending = scheduled.values().toArray(Scheduled[]::new);
            scheduled.clear();
        }
        // 서버 종료 시 실행 완료를 기다리지 않고 구독을 취소한다. 방 정리 시에는 실행을 취소하지 않는다.
        runningAutoPlays.dispose();
        for (Scheduled timer : pending) {
            timer.task().dispose();
        }
    }

    // RoomCleanupService의 직접 의존에 따른 순환 참조를 피하기 위해 방 정리 이벤트를 수신한다.
    @EventListener
    public void onRoomCleanedUp(RoomCleanedUpEvent event) {
        synchronized (timerLifecycle) {
            timerLifecycle.close(event.roomId());
            cancelAutoPlay(event.roomId());
        }
    }

    @Override
    public void cancelAutoPlay(long roomId) {
        synchronized (timerLifecycle) {
            Scheduled removed = scheduled.remove(roomId);
            if (removed != null && removed.task != null && !removed.task.isDisposed()) {
                removed.task.dispose();
            }
        }
    }

    private Mono<Void> attemptAutoPlay(long roomId, TurnStep step, Player currentPlayer,
                                       TurnScheduler boundScheduler, Scheduled fired) {
        return gameService.findGameState(roomId)
                .flatMap(gameState -> {
                    if (!step.matches(gameState)) {
                        return Mono.empty();
                    }

                    // 사용자 요청이 진행 중이면 1초 뒤에 다시 확인한다.
                    String normalFlagKey = InFlightManager.normalKey(roomId, currentPlayer.getNumber());

                    return inFlightManager.isSet(normalFlagKey)
                            .flatMap(isDelayed -> {
                                if (isDelayed) {
                                    return Mono.delay(Duration.ofSeconds(1))
                                            .then(Mono.defer(() -> attemptAutoPlay(roomId, step, currentPlayer, boundScheduler, fired)));
                                } else {
                                    return executeAutoPlayLogic(roomId, step, currentPlayer, boundScheduler)
                                            .onErrorResume(WebSocketBusinessException.class, error -> {
                                                if (error.getWebsocketErrorCode() != WebSocketErrorCode.TRY_AGAIN) {
                                                    return Mono.error(error);
                                                }
                                                // 읽기 락 경합도 다음 턴을 만들지 않는다. 원래 방 수명에서만 다시 예약한다.
                                                synchronized (timerLifecycle) {
                                                    if (fired != null && scheduled.get(roomId) == fired) {
                                                        boundScheduler.scheduleAutoPlay(roomId, step.round(), step.turn(), currentPlayer,
                                                                System.nanoTime() + Duration.ofSeconds(1).toNanos(), step.phase());
                                                    }
                                                }
                                                return Mono.empty();
                                            });
                                }
                            });
                });
    }

    private Mono<Void> executeAutoPlayLogic(long roomId, TurnStep step, Player currentPlayer, TurnScheduler boundScheduler) {
        // AUTOPLAY 키는 사용자 요청의 키와 분리하며, 자동플레이 간 동시 시작만 막는다.
        String autoplayFlagKey = InFlightManager.autoplayKey(roomId, currentPlayer.getNumber());
        String normalFlagKey = InFlightManager.normalKey(roomId, currentPlayer.getNumber());
        // 실행마다 소유 토큰을 발급해 TTL 만료 후 다른 실행이 획득한 플래그를 삭제하지 않게 한다.
        String autoplayToken = Long.toHexString(ThreadLocalRandom.current().nextLong());
        return inFlightManager.trySetFlag(autoplayFlagKey, autoplayToken, Duration.ofSeconds(2))
                .flatMap(acquired -> {
                    if (!acquired) return Mono.empty();

                    Mono<Void> mainProcess = Mono.defer(() -> inFlightManager.isSet(normalFlagKey)
                            .flatMap(normalInProgress -> {
                                // 앞선 확인 이후 사용자 요청이 시작됐을 수 있으므로 게임 실행 직전에 다시 확인한다.
                                if (normalInProgress) return Mono.<Void>empty();
                                return gameService.findGameState(roomId)
                                        .flatMap(gameState -> {
                                            if (!step.matches(gameState)) {
                                                return Mono.empty();
                                            }

                                            return switch (step.phase()) {
                                                case AWAITING_FLOOR_CARD_CHOICE ->
                                                        turnFlowService.processFloorSelection(roomId, currentPlayer, AUTO_PLAY_CARD_INDEX, GameActionSource.AUTOPLAY, boundScheduler);
                                                case AWAITING_GO_STOP_CHOICE ->
                                                        turnFlowService.processGoStopChoice(roomId, currentPlayer, AUTO_GO_STOP_IS_GO, GameActionSource.AUTOPLAY, boundScheduler);
                                                default ->
                                                        turnFlowService.processNormalSubmit(roomId, currentPlayer, AUTO_PLAY_CARD_INDEX, GameActionSource.AUTOPLAY, boundScheduler);
                                            };
                                        });
                            }));

                    return Mono.usingWhen(
                            Mono.just(autoplayFlagKey),
                            key -> mainProcess,
                            key -> inFlightManager.deleteFlag(key, autoplayToken)
                    ).then();
                });
    }
}
