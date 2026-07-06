package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.api.response.websocket.GameMessageSender;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.GameNotificationService;
import com.pomingmatgo.gameservice.domain.service.matgo.GamePlayService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
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

import static com.pomingmatgo.gameservice.domain.GamePhase.IN_PROGRESS;
import static com.pomingmatgo.gameservice.domain.TurnTiming.GRACE_PERIOD_MILLIS;
import static com.pomingmatgo.gameservice.domain.TurnTiming.TURN_TIMEOUT_MILLIS;

/**
 * 자동플레이 스케줄러.
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
public class AutoPlayScheduler {

    private final InFlightManager inFlightManager;
    private final RoomService roomService;
    private final GamePlayService gamePlayService;
    private final GameMessageSender gameMessageSender;
    private final GameNotificationService gameNotificationService;

    private record Scheduled(int sequence, Disposable task) {}

    private final Map<Long, Scheduled> scheduled = new ConcurrentHashMap<>();

    private int getTurnSequence(int round, int turn) {
        return (round * 10000) + turn;
    }


    public void scheduleAutoPlay(long roomId, int round, int currentTurn, Player currentPlayer, long deadlineNanos) {
        int newSequence = getTurnSequence(round, currentTurn);

        long delayMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (delayMillis <= 0) delayMillis = 100;

        Disposable newTask = Mono.delay(Duration.ofMillis(delayMillis))
                .flatMap(v -> attemptAutoPlay(roomId, round, currentTurn, currentPlayer))
                .subscribe(
                        success -> {},
                        error -> log.error("[AutoPlay] 룸({}) 자동플레이 스케줄링 중 에러 발생!", roomId, error)
                );

        Disposable[] toDispose = new Disposable[1];
        scheduled.compute(roomId, (k, prev) -> {
            if (prev != null && prev.sequence > newSequence) {
                toDispose[0] = newTask;
                return prev;
            }
            toDispose[0] = (prev != null) ? prev.task : null;
            return new Scheduled(newSequence, newTask);
        });

        if (toDispose[0] != null && !toDispose[0].isDisposed()) {
            toDispose[0].dispose();
        }
    }

    public void cancelAutoPlay(long roomId) {
        Scheduled removed = scheduled.remove(roomId);
        if (removed != null && removed.task != null && !removed.task.isDisposed()) {
            removed.task.dispose();
        }
    }

    private Mono<Void> attemptAutoPlay(long roomId, int round, int currentTurn, Player currentPlayer) {
        return roomService.getGameState(roomId)
                .flatMap(gameState -> {
                    if (gameState.getRound() != round || gameState.getCurrentTurn() != currentTurn || gameState.getPhase() != IN_PROGRESS) {
                      //  log.warn("[AutoPlay] 턴 불일치 또는 상태 이상 - expected:({}, {}), actual:({}, {}), phase:{}",
                      //          round, currentTurn, gameState.getRound(), gameState.getCurrentTurn(), gameState.getPhase());
                        return Mono.empty();
                    }

                    // 정상 요청 진행 여부는 NORMAL 키로 체크 (양보)
                    String normalFlagKey = "IN_FLIGHT:NORMAL:ROOM:" + roomId + ":PLAYER:" + currentPlayer.getNumber();

                    return inFlightManager.isSet(normalFlagKey)
                            .flatMap(isDelayed -> {
                                if (isDelayed) {
                                    return Mono.delay(Duration.ofSeconds(1))
                                            .then(Mono.defer(() -> attemptAutoPlay(roomId, round, currentTurn, currentPlayer)));
                                } else {
                                    return executeAutoPlayLogic(roomId, round, currentTurn, currentPlayer);
                                }
                            });
                });
    }

    private Mono<Void> executeAutoPlayLogic(long roomId, int round, int turnNumber, Player currentPlayer) {
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
                                            if (gameState.getRound() != round || gameState.getCurrentTurn() != turnNumber || gameState.getPhase() != IN_PROGRESS) {
                                                return Mono.empty();
                                            }

                                            int autoCardIdx = 0;

                                            return gamePlayService.executeNormalSubmit(roomId, gameState, currentPlayer, autoCardIdx, () -> {})
                                                    .flatMap(ctx -> {
                                                        Mono<Void> sendInfos = Mono.when(
                                                                gameMessageSender.sendSubmitCardInfo(roomId, currentPlayer, ctx.submittedCard()),
                                                                gameMessageSender.sendTopCardInfo(roomId, currentPlayer, ctx.topCard())
                                                        );

                                                        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TURN_TIMEOUT_MILLIS + GRACE_PERIOD_MILLIS);
                                                        Mono<Void> handleResult;

                                                        if (ctx.isChoiceRequired()) {
                                                            handleResult = gameMessageSender.sendChooseFloorCardMessage(roomId, currentPlayer, ctx.cardResult().getSelectableCards());
                                                        } else {
                                                            handleResult = gameNotificationService.broadcastTurnResult(roomId, currentPlayer, ctx.updatedGameState(), ctx.cardResult(), () -> this.cancelAutoPlay(roomId), TURN_TIMEOUT_MILLIS)
                                                                    .doOnNext(nextState -> {
                                                                        if (nextState.getPhase() == IN_PROGRESS) {
                                                                            scheduleAutoPlay(
                                                                                    roomId, nextState.getRound(), nextState.getCurrentTurn(), nextState.getCurrentPlayer(), deadlineNanos
                                                                            );
                                                                        }
                                                                    })
                                                                    .then();
                                                        }

                                                        return sendInfos.then(handleResult);
                                                    });
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