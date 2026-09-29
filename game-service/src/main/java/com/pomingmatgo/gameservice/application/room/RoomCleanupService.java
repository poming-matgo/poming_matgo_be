package com.pomingmatgo.gameservice.application.room;

import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.event.GameActionFailedEvent;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.infrastructure.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

@Service
@RequiredArgsConstructor
@Log4j2
public class RoomCleanupService {
    private static final Duration TERMINATION_NOTIFICATION_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration CLEANUP_STEP_TIMEOUT = Duration.ofSeconds(30);

    private final GameStateRepository gameStateRepository;
    private final InstalledCardRepository installedCardRepository;
    private final AcquiredCardRepository acquiredCardRepository;
    private final LeadingPlayerRepository leadingPlayerRepository;
    private final GameLockCleaner gameLockCleaner;
    private final ApplicationEventPublisher eventPublisher;
    private final SessionManager sessionManager;
    private final Map<Long, CleanupExecution> executions = new HashMap<>();
    private boolean stopped;

    /** 최초 구독 시 정리를 시작하며, 결과 관찰자의 취소는 이미 시작한 정리를 중단하지 않는다. */
    public Mono<Void> cleanupRoom(long roomId) {
        return cleanupRoom(roomId, Mono.empty());
    }

    /** 최초 요청의 종료 안내를 최대 5초 기다린 뒤 정리하며, 중복 요청은 기존 실행을 관찰한다. */
    public Mono<Void> cleanupRoom(long roomId, Mono<Void> notification) {
        return Mono.defer(() -> startCleanup(roomId, notification));
    }

    @EventListener
    public void onGameActionFailed(GameActionFailedEvent event) {
        // 동기 리스너에서 기존 관리 실행을 시작한다. gate 해제를 기다리는 결과에는 구독하지 않는다.
        startCleanup(event.roomId(), Mono.empty());
    }

    private Mono<Void> startCleanup(long roomId, Mono<Void> notification) {
        CleanupExecution execution;
        synchronized (executions) {
            if (stopped) throw new IllegalStateException("Room cleanup service stopped");
            execution = executions.get(roomId);
            if (execution != null) return execution.result.asMono();
            execution = new CleanupExecution(roomId);
            executions.put(roomId, execution);
        }
        // 호출자는 결과만 관찰한다. 실제 정리 구독은 방별로 하나만 소유한다.
        Mono.defer(() -> gameLockCleaner.withCleanup(roomId, () -> cleanup(roomId, notification)))
                .subscribe(execution);
        return execution.result.asMono();
    }

    private Mono<Void> cleanup(long roomId, Mono<Void> notification) {
        // 안내·데이터 정리 오류 뒤에도 다음 단계를 시도하고, 각 단계의 오류를 보존한다.
        return Flux.concatDelayError(
                Mono.defer(() -> notification).timeout(TERMINATION_NOTIFICATION_TIMEOUT),
                Mono.defer(() -> deleteRoomData(roomId)),
                Mono.defer(() -> sessionManager.removeRoom(roomId)).timeout(CLEANUP_STEP_TIMEOUT)
        ).then().doOnError(error -> log.error("Room ({}) cleanup failed", roomId, error));
    }

    @PreDestroy
    public void shutdown() {
        List<CleanupExecution> pending;
        synchronized (executions) {
            stopped = true;
            pending = List.copyOf(executions.values());
        }
        pending.forEach(CleanupExecution::dispose);
    }

    private final class CleanupExecution extends BaseSubscriber<Void> {
        private final long roomId;
        private final Sinks.Empty<Void> result = Sinks.empty();

        private CleanupExecution(long roomId) {
            this.roomId = roomId;
        }

        private void remove() {
            synchronized (executions) {
                executions.remove(roomId, this);
            }
        }

        @Override
        protected void hookOnComplete() {
            remove();
            result.tryEmitEmpty();
        }

        @Override
        protected void hookOnError(Throwable error) {
            remove();
            result.tryEmitError(error);
        }

        @Override
        protected void hookOnCancel() {
            remove();
            result.tryEmitError(new CancellationException("Room cleanup stopped: " + roomId));
        }
    }

    public Mono<Void> restartRoom(long roomId) {
        // withCleanup은 현재 실행의 해제를 기다리므로 재시작 소유 구간에서 중첩 호출하지 않는다.
        return Mono.defer(() -> gameLockCleaner.withRestart(roomId, () -> deleteRoomData(roomId)
                .then(Mono.defer(() -> gameStateRepository.create(GameState.createEmptyRoom(roomId))))
                .then()));
    }

    private Mono<Void> deleteRoomData(long roomId) {
        // 전체 정리 또는 재시작의 보호 구간 안에서만 호출한다.
        // 개별 오류가 다른 정리를 취소하지 않게 하고, 동기 예외도 구독 시 오류로 합산한다.
        return Mono.whenDelayError(
                // 데이터 삭제 전에 신규 타이머를 차단한다. 동기 리스너로 등록과 종료를 직렬화한다.
                Mono.fromRunnable(() -> eventPublisher.publishEvent(new RoomCleanedUpEvent(roomId))).timeout(CLEANUP_STEP_TIMEOUT),
                // 집계 바깥의 timeout은 먼저 발생한 오류를 잃으므로 각 정리를 개별 제한한다.
                Mono.defer(() -> gameStateRepository.cleanup(roomId)).timeout(CLEANUP_STEP_TIMEOUT),
                Mono.defer(() -> installedCardRepository.cleanup(roomId)).timeout(CLEANUP_STEP_TIMEOUT),
                Mono.defer(() -> acquiredCardRepository.cleanup(roomId)).timeout(CLEANUP_STEP_TIMEOUT),
                Mono.defer(() -> leadingPlayerRepository.cleanup(roomId)).timeout(CLEANUP_STEP_TIMEOUT),
                Mono.defer(() -> gameLockCleaner.cleanup(roomId)).timeout(CLEANUP_STEP_TIMEOUT)
        ).doOnError(error -> log.error("Room ({}) data cleanup failed", roomId, error));
    }
}
