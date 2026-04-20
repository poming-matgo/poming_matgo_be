package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.api.response.websocket.GameMessageSender;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.GameNotificationService;
import com.pomingmatgo.gameservice.domain.service.matgo.GamePlayService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.pomingmatgo.gameservice.domain.GamePhase.IN_PROGRESS;

@Service
@RequiredArgsConstructor
@Log4j2
public class AutoPlayScheduler {

    private final RedissonReactiveClient redissonReactiveClient;
    private final RoomService roomService;
    private final GamePlayService gamePlayService;
    private final GameMessageSender gameMessageSender;
    private final GameNotificationService gameNotificationService;

    private final Map<Long, Disposable> autoPlayTasks = new ConcurrentHashMap<>();

    private final Map<Long, Integer> scheduledSequences = new ConcurrentHashMap<>();

    private int getTurnSequence(int round, int turn) {
        return (round * 10000) + turn;
    }


    public void scheduleAutoPlay(long roomId, int round, int currentTurn, Player currentPlayer, long deadlineMillis) {
        int newSequence = getTurnSequence(round, currentTurn);
        int existingSequence = scheduledSequences.getOrDefault(roomId, 0);

        if (newSequence < existingSequence) {
            log.warn("[AutoPlay] 예약 무시됨: 이미 더 최신 턴이 예약되어 있습니다. 기존: {}, 무시된 요청: (라운드:{}, 턴:{})",
                    existingSequence, round, currentTurn);
            return;
        }

        cancelAutoPlay(roomId);

        scheduledSequences.put(roomId, newSequence);

        long delayMillis = deadlineMillis - System.currentTimeMillis();
        if (delayMillis <= 0) delayMillis = 100;

        Disposable task = Mono.delay(Duration.ofMillis(delayMillis))
                .flatMap(v -> attemptAutoPlay(roomId, round, currentTurn, currentPlayer))
                .subscribe(
                        success -> {},
                        error -> log.error("[AutoPlay] 룸({}) 자동플레이 스케줄링 중 에러 발생!", roomId, error)
                );

        autoPlayTasks.put(roomId, task);
    }

    public void cancelAutoPlay(long roomId) {
        Disposable task = autoPlayTasks.remove(roomId);
        if (task != null && !task.isDisposed()) {
            task.dispose();
        }
    }

    private Mono<Void> attemptAutoPlay(long roomId, int round, int currentTurn, Player currentPlayer) {
        return roomService.getGameState(roomId)
                .flatMap(gameState -> {
                    if (gameState.getRound() != round || gameState.getCurrentTurn() != currentTurn || gameState.getPhase() != IN_PROGRESS) {
                        log.warn("[AutoPlay] 턴 불일치 또는 상태 이상 - expected:({}, {}), actual:({}, {}), phase:{}",
                                round, currentTurn, gameState.getRound(), gameState.getCurrentTurn(), gameState.getPhase());
                        return Mono.empty();
                    }

                    String flagKey = "IN_FLIGHT:ROOM:" + roomId + ":PLAYER:" + currentPlayer.getNumber();

                    return redissonReactiveClient.getBucket(flagKey).isExists()
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
        String flagKey = "IN_FLIGHT:ROOM:" + roomId + ":PLAYER:" + currentPlayer.getNumber();
        return redissonReactiveClient.getBucket(flagKey).setIfAbsent("AUTO_PLAY", Duration.ofSeconds(2))
                .flatMap(acquired -> {
                    if (!acquired) return Mono.empty();

                    Mono<Void> mainProcess = Mono.defer(() -> roomService.getGameState(roomId)
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

                                            long TURN_TIMEOUT_MILLIS = 10000L;
                                            long deadlineMillis = System.currentTimeMillis() + TURN_TIMEOUT_MILLIS;
                                            Mono<Void> handleResult;

                                            if (ctx.isChoiceRequired()) {
                                                handleResult = gameMessageSender.sendChooseFloorCardMessage(roomId, currentPlayer, ctx.cardResult().getSelectableCards());
                                            } else {
                                                handleResult = gameNotificationService.broadcastTurnResult(roomId, currentPlayer, ctx.updatedGameState(), ctx.cardResult(), () -> this.cancelAutoPlay(roomId))
                                                        .doOnNext(nextState -> {
                                                            if (nextState.getPhase() == IN_PROGRESS) {
                                                                scheduleAutoPlay(
                                                                        roomId, nextState.getRound(), nextState.getCurrentTurn(), nextState.getCurrentPlayer(), deadlineMillis
                                                                );
                                                            }
                                                        })
                                                        .then();
                                            }

                                            return sendInfos.then(handleResult);
                                        });
                            }));

                    return Mono.usingWhen(
                            Mono.just(flagKey),
                            key -> mainProcess,
                            key -> redissonReactiveClient.getBucket(key).delete()
                    ).then();
                });
    }
}