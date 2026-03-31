package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import lombok.RequiredArgsConstructor;
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
public class AutoPlayScheduler {

    private final RedissonReactiveClient redissonClient;
    private final RoomService roomService;

    private final Map<Long, Disposable> autoPlayTasks = new ConcurrentHashMap<>();

    public void scheduleAutoPlay(long roomId, int currentTurn, Player currentPlayer, long deadlineMillis) {
        cancelAutoPlay(roomId);

        long delayMillis = deadlineMillis - System.currentTimeMillis();
        if (delayMillis <= 0) delayMillis = 100;

        Disposable task = Mono.delay(Duration.ofMillis(delayMillis))
                .flatMap(v -> attemptAutoPlay(roomId, currentTurn, currentPlayer, deadlineMillis))
                .subscribe();

        autoPlayTasks.put(roomId, task);
    }

    public void cancelAutoPlay(long roomId) {
        Disposable task = autoPlayTasks.remove(roomId);
        if (task != null && !task.isDisposed()) {
            task.dispose();
        }
    }

    private Mono<Void> attemptAutoPlay(long roomId, int currentTurn, Player currentPlayer, long deadlineMillis) {
        return roomService.getGameState(roomId)
                .flatMap(gameState -> {
                    if (gameState.getCurrentTurn() != currentTurn || gameState.getPhase() != IN_PROGRESS) {
                        return Mono.empty();
                    }

                    String flagKey = "IN_FLIGHT:ROOM:" + roomId;

                    return redissonClient.getBucket(flagKey).get()
                            .map(flagArrivalTime -> (long) flagArrivalTime <= deadlineMillis)
                            .defaultIfEmpty(false)
                            .flatMap(isDelayed -> {
                                if (isDelayed) {
                                    return Mono.delay(Duration.ofSeconds(1))
                                            .then(Mono.defer(() -> attemptAutoPlay(roomId, currentTurn, currentPlayer, deadlineMillis)));
                                } else {
                                    return executeAutoPlayLogic(roomId, currentTurn, currentPlayer);
                                }
                            });
                });
    }

    private Mono<Void> executeAutoPlayLogic(long roomId, int turnNumber, Player currentPlayer) {
        return Mono.empty();
    }
}