package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 자동플레이 스케줄러.
 *
 * 타이머의 정체성은 TurnStep(round, turn, phase) 하나로 표현한다:
 * - IN_PROGRESS: 턴 타임아웃 시 카드 자동 제출
 * - AWAITING_FLOOR_CARD_CHOICE: 선택 타임아웃 시 바닥 카드 자동 선택
 * - AWAITING_GO_STOP_CHOICE: 선택 타임아웃 시 자동 STOP (확정된 승리를 가져가는 안전한 기본값 + 게임 종료 보장)
 * 등록 시점엔 TurnStep의 순서 비교로 낡은 등록이 유효한 타이머를 교체(파괴)하지 못하게 막고,
 * 발사 시점엔 실제 게임 상태가 TurnStep과 일치하는지 재검증해 낡은 타이머가 스스로 물러나게 한다.
 * 실행 자체는 TurnFlowService에 위임하므로 후처리(타이머 재등록 포함)는 사용자 요청 경로와 동일하다.
 *
 * 제약: scheduled 맵과 reactor.core.Disposable 기반 타이머는 모두 인스턴스 로컬이다.
 * 따라서 다수 인스턴스 배포 시  방 단위 sticky routing이 전제되어야 한다.
 * (한 방의 모든 메시지가 항상 같은 인스턴스로 라우팅).
 *
 * 이 전제가 깨지면 한 인스턴스의 cancelAutoplay가 다른 인스턴스의 타이머를 취소하지 못해
 * false 자동플레이가 발사될 수 있다. 분산 환경에서 sticky routing 없이 동작시키려면 Redis 기반 분산 타이머가 별도로 필요하다.
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class AutoPlayScheduler implements TurnScheduler {

    // 자동 액션은 항상 첫 번째 카드(손패/선택지)를 사용
    private static final int AUTO_PLAY_CARD_INDEX = 0;
    // 고/스톱 타임아웃 시 자동 선택 — STOP(false): 확정 승리로 즉시 종료 (GO는 AFK 플레이어의 리스크를 키움)
    private static final boolean AUTO_GO_STOP_IS_GO = false;
    // 타이머 발사 시점이 이미 deadline을 지났을 때 즉시 실행 대신 주는 최소 지연
    private static final long MIN_DELAY_MILLIS = 100;

    private final InFlightManager inFlightManager;
    private final RoomService roomService;
    private final TurnFlowService turnFlowService;

    /**
     * 타이머가 속한 턴 단계의 정체성. 등록 시점의 교체 판정(순서 비교)과
     * 발사 시점의 상태 재검증(matches)이 같은 값을 공유한다.
     * 같은 턴 안의 단계 순서는 GamePhase.turnStepOrder가 정의한다
     * (제출 < 바닥 선택 < 고/스톱 선택) — 낡은 앞 단계 타이머 등록이
     * 먼저 등록된 뒤 단계 타이머를 교체(파괴)하지 못하게 한다.
     */
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

    private record Scheduled(TurnStep step, Disposable task) {}

    private final Map<Long, Scheduled> scheduled = new ConcurrentHashMap<>();

    @Override
    public void scheduleAutoPlay(long roomId, int round, int currentTurn, Player currentPlayer, long deadlineNanos, GamePhase expectedPhase) {
        TurnStep newStep = new TurnStep(round, currentTurn, expectedPhase);

        long delayMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (delayMillis <= 0) delayMillis = MIN_DELAY_MILLIS;

        // 취소(dispose) 대상은 "대기 중인 타이머"로 한정한다. 발사 이후의 실행은 독립 구독으로 분리 —
        // 실행 도중 cancelAutoPlay가 호출되는 경우(자동플레이 자신의 게임 종료 cleanup, 고/스톱 분기 등)
        // 실행 체인을 중단시키면 아직 전송되지 않은 GAME_OVER/GO_STOP_CHOICE 메시지가 유실된다.
        // 발사 이후의 경합은 TurnStep 재검증 + InFlight + @GameLock이 방어한다.
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
            return new Scheduled(newStep, newTask);
        });

        if (toDispose[0] != null && !toDispose[0].isDisposed()) {
            toDispose[0].dispose();
        }
    }

    @Override
    public void cancelAutoPlay(long roomId) {
        Scheduled removed = scheduled.remove(roomId);
        if (removed != null && removed.task != null && !removed.task.isDisposed()) {
            removed.task.dispose();
        }
    }

    private Mono<Void> attemptAutoPlay(long roomId, TurnStep step, Player currentPlayer) {
        return roomService.getGameState(roomId)
                .flatMap(gameState -> {
                    if (!step.matches(gameState)) {
                        return Mono.empty();
                    }

                    // 정상 요청 진행 여부는 NORMAL 키로 체크 (양보)
                    String normalFlagKey = "IN_FLIGHT:NORMAL:ROOM:" + roomId + ":PLAYER:" + currentPlayer.getNumber();

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
        // AUTOPLAY 키는 자동플레이끼리의 동시 시작 방지용 (정상 요청과는 키 분리)
        String autoplayFlagKey = "IN_FLIGHT:AUTOPLAY:ROOM:" + roomId + ":PLAYER:" + currentPlayer.getNumber();
        String normalFlagKey = "IN_FLIGHT:NORMAL:ROOM:" + roomId + ":PLAYER:" + currentPlayer.getNumber();
        // 발사별 소유 토큰: TTL 만료 후 다른 발사가 플래그를 재획득해도 내 정리가 남의 플래그를 지우지 않게 함
        String autoplayToken = Long.toHexString(ThreadLocalRandom.current().nextLong());
        return inFlightManager.trySetFlag(autoplayFlagKey, autoplayToken, Duration.ofSeconds(2))
                .flatMap(acquired -> {
                    if (!acquired) return Mono.empty();

                    Mono<Void> mainProcess = Mono.defer(() -> inFlightManager.isSet(normalFlagKey)
                            .flatMap(normalInProgress -> {
                                // attempt 시점 이후 정상 요청이 막 도착했을 수 있음 → 게임 로직 진입 직전 한 번 더 체크 (race 좁힘)
                                if (normalInProgress) return Mono.<Void>empty();
                                return roomService.getGameState(roomId)
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
