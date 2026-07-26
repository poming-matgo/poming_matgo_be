package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.TurnTiming;
import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

// scheduled 맵과 Disposable 타이머가 모두 인스턴스 로컬 — 다중 인스턴스 배포는 방 단위 sticky routing 전제.
// 깨지면 한 인스턴스의 cancelAutoPlay가 다른 인스턴스의 타이머를 취소하지 못해 false 자동플레이가 발사된다
@Service
@RequiredArgsConstructor
@Log4j2
public class AutoPlayScheduler implements TurnScheduler {

    private static final int AUTO_PLAY_CARD_INDEX = 0;
    // STOP: 확정 승리로 즉시 종료 — GO는 AFK 플레이어의 리스크를 키운다
    private static final boolean AUTO_GO_STOP_IS_GO = false;
    private static final long MIN_DELAY_MILLIS = 100;

    private final InFlightManager inFlightManager;
    private final GameService gameService;
    private final TurnFlowService turnFlowService;

    // 등록 시 교체 판정(순서 비교)과 발사 시 상태 재검증(matches)이 공유하는 타이머 정체성.
    // 순서는 GamePhase.turnStepOrder — 낡은 앞 단계 등록이 먼저 등록된 뒤 단계 타이머를 파괴하지 못하게 한다
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

    /** 재접속 스냅샷 표시용 근사값 — deadline이 품은 GRACE_PERIOD를 뺀다. 실제 타임아웃 판정은 타이머 자신이 한다 */
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
        TurnStep newStep = new TurnStep(round, currentTurn, expectedPhase);

        long delayMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (delayMillis <= 0) delayMillis = MIN_DELAY_MILLIS;

        // 발사 이후 실행은 독립 구독 — 실행 체인을 dispose하면 미전송 GAME_OVER/GO_STOP_CHOICE가 유실된다.
        // 발사 이후 경합은 TurnStep 재검증 + InFlight + @GameLock이 방어하므로 취소 대상은 대기 중인 타이머뿐
        Disposable newTask = Mono.delay(Duration.ofMillis(delayMillis))
                .subscribe(v -> attemptAutoPlay(roomId, newStep, currentPlayer)
                        .subscribe(
                                success -> {},
                                error -> log.error("[AutoPlay] 룸({}) 자동플레이 실행 중 에러 발생!", roomId, error)
                        ));

        // 같은 단계의 재등록(연속 바닥 카드 선택)은 교체를 허용해야 하므로 초과(>)일 때만 기존 유지
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

    // RoomCleanupService가 이 클래스를 직접 의존하면 DI cycle이 생기므로 이벤트로 수신한다
    @EventListener
    public void onRoomCleanedUp(RoomCleanedUpEvent event) {
        cancelAutoPlay(event.roomId());
    }

    @Override
    public void cancelAutoPlay(long roomId) {
        Scheduled removed = scheduled.remove(roomId);
        if (removed != null && removed.task != null && !removed.task.isDisposed()) {
            removed.task.dispose();
        }
    }

    private Mono<Void> attemptAutoPlay(long roomId, TurnStep step, Player currentPlayer) {
        return gameService.findGameState(roomId)
                .flatMap(gameState -> {
                    if (!step.matches(gameState)) {
                        return Mono.empty();
                    }

                    // 정상 요청이 진행 중이면 양보한다
                    String normalFlagKey = InFlightManager.normalKey(roomId, currentPlayer.getNumber());

                    return inFlightManager.isSet(normalFlagKey)
                            .flatMap(isDelayed -> {
                                if (isDelayed) {
                                    return Mono.delay(Duration.ofSeconds(1))
                                            .then(Mono.defer(() -> attemptAutoPlay(roomId, step, currentPlayer)));
                                } else {
                                    return executeAutoPlayLogic(roomId, step, currentPlayer);
                                }
                            });
                });
    }

    private Mono<Void> executeAutoPlayLogic(long roomId, TurnStep step, Player currentPlayer) {
        // AUTOPLAY 키는 자동플레이끼리의 동시 시작만 막는다 (정상 요청과 키 분리)
        String autoplayFlagKey = InFlightManager.autoplayKey(roomId, currentPlayer.getNumber());
        String normalFlagKey = InFlightManager.normalKey(roomId, currentPlayer.getNumber());
        // 발사별 소유 토큰 — TTL 만료 후 다른 발사가 플래그를 재획득해도 내 정리가 남의 플래그를 지우지 않는다
        String autoplayToken = Long.toHexString(ThreadLocalRandom.current().nextLong());
        return inFlightManager.trySetFlag(autoplayFlagKey, autoplayToken, Duration.ofSeconds(2))
                .flatMap(acquired -> {
                    if (!acquired) return Mono.empty();

                    Mono<Void> mainProcess = Mono.defer(() -> inFlightManager.isSet(normalFlagKey)
                            .flatMap(normalInProgress -> {
                                // attempt 이후 정상 요청이 막 도착했을 수 있어 진입 직전 재확인 (race 좁힘)
                                if (normalInProgress) return Mono.<Void>empty();
                                return gameService.findGameState(roomId)
                                        .flatMap(gameState -> {
                                            if (!step.matches(gameState)) {
                                                return Mono.empty();
                                            }

                                            return switch (step.phase()) {
                                                case AWAITING_FLOOR_CARD_CHOICE ->
                                                        turnFlowService.processFloorSelection(roomId, gameState, currentPlayer, AUTO_PLAY_CARD_INDEX, null, this);
                                                case AWAITING_GO_STOP_CHOICE ->
                                                        turnFlowService.processGoStopChoice(roomId, gameState, currentPlayer, AUTO_GO_STOP_IS_GO, null, this);
                                                default ->
                                                        turnFlowService.processNormalSubmit(roomId, gameState, currentPlayer, AUTO_PLAY_CARD_INDEX, null, this);
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
