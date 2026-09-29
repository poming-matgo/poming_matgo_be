package com.pomingmatgo.gameservice.application.game;

import com.pomingmatgo.gameservice.domain.event.GameActionFailedEvent;
import com.pomingmatgo.gameservice.infrastructure.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.MonoSink;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

@Profile("in-memory")
@Slf4j
@Component
@RequiredArgsConstructor
public class InMemoryGameActionExecutor implements GameLockCleaner {
    private static final Duration EXECUTION_TIMEOUT = Duration.ofSeconds(30);

    private final InMemoryRoomExecutionGate executionGate;
    private final ApplicationEventPublisher eventPublisher;
    private final Set<ActionExecution> executions = new HashSet<>();
    private boolean stopped;

    // 수락 이후 호출자 취소와 분리하는 범위는 락 내부 실행과 해제까지다.
    public Mono<Object> execute(long roomId, Supplier<Mono<Object>> operation) {
        return Mono.create(sink -> {
            ActionExecution execution = new ActionExecution(roomId, sink);
            synchronized (executions) {
                if (stopped) {
                    sink.error(new IllegalStateException("Game action execution stopped"));
                    return;
                }
                executions.add(execution);
            }
            sink.onCancel(execution::cancelCaller);
            if (execution.isDisposed()) return;
            // 연결은 결과를 관찰하고, 수락한 실행과 락은 서비스가 완료까지 소유한다.
            locked(operation, roomId, execution).subscribe(execution);
        });
    }

    private Mono<Object> locked(Supplier<Mono<Object>> operation,
                                long roomId, ActionExecution execution) {
        return Mono.usingWhen(
                Mono.fromSupplier(() -> executionGate.acquire(roomId)),
                s -> Mono.defer(operation).contextWrite(context -> context.put(GameActionAcceptance.class,
                        (BooleanSupplier) () -> execution.accept(s)).put(ActionExecution.class, execution))
                        // 검증부터 필수 후처리까지 제한한다. timeout도 해제 전에 수락 후 오류로 차단한다.
                        .timeout(EXECUTION_TIMEOUT)
                        .doOnError(error -> {
                            if (execution.isAccepted()) {
                                executionGate.fail(s);
                                // 해제 뒤 발행하면 먼저 끝난 정리와 재생성 이후의 새 방을 지울 수 있다.
                                try {
                                    eventPublisher.publishEvent(new GameActionFailedEvent(roomId));
                                } catch (RuntimeException cleanupError) {
                                    log.error("Room ({}) failed action cleanup could not start; room remains blocked",
                                            roomId, cleanupError);
                                }
                            }
                        }),
                s -> Mono.fromRunnable(() -> execution.release(s)),
                (s, err) -> Mono.fromRunnable(() -> execution.release(s)),
                s -> Mono.fromRunnable(() -> execution.release(s))
        );
    }

    @PreDestroy
    public void shutdown() {
        List<ActionExecution> pending;
        synchronized (executions) {
            stopped = true;
            pending = List.copyOf(executions);
        }
        pending.forEach(ActionExecution::stop);
    }

    private final class ActionExecution extends BaseSubscriber<Object> {
        private final long roomId;
        private final MonoSink<Object> result;
        private boolean accepted;
        private boolean cancelled;
        private InMemoryRoomExecutionGate.Entry entry;
        private Object value;
        private final AtomicBoolean terminated = new AtomicBoolean();

        private ActionExecution(long roomId, MonoSink<Object> result) {
            this.roomId = roomId;
            this.result = result;
        }

        private synchronized boolean accept(InMemoryRoomExecutionGate.Entry entry) {
            if (cancelled) return false;
            if (!accepted) {
                executionGate.accept(entry);
                this.entry = entry;
                accepted = true;
            }
            return true;
        }

        private synchronized boolean isAccepted() {
            return accepted;
        }

        private synchronized Mono<Void> restart(long targetRoomId, Supplier<Mono<Void>> operation) {
            if (targetRoomId != roomId || entry == null || !accepted || cancelled || terminated.get()) {
                return Mono.error(new IllegalStateException("Restart requires an active accepted action: " + targetRoomId));
            }
            // 현재 gate를 유지한 채 재시작한다. 별도 획득이나 withCleanup 대기는 자기 경합을 만든다.
            return executionGate.inRestart(entry, operation);
        }

        private void release(InMemoryRoomExecutionGate.Entry acquired) {
            synchronized (this) {
                // gate 해제가 대기 중 정리를 깨우기 전에 현재 실행의 재시작 권한을 무효화한다.
                entry = null;
            }
            executionGate.release(acquired);
        }

        private void cancelCaller() {
            synchronized (this) {
                if (accepted) return;
                cancelled = true;
            }
            dispose();
            hookOnCancel();
        }

        private void stop() {
            synchronized (this) {
                cancelled = true;
            }
            dispose();
            hookOnCancel();
        }

        private void remove() {
            synchronized (executions) {
                executions.remove(this);
            }
        }

        @Override
        public Context currentContext() {
            return Context.of(result.contextView());
        }

        @Override
        protected void hookOnNext(Object value) {
            this.value = value;
        }

        @Override
        protected void hookOnComplete() {
            if (!terminated.compareAndSet(false, true)) return;
            remove();
            result.success(value);
        }

        @Override
        protected void hookOnError(Throwable error) {
            if (!terminated.compareAndSet(false, true)) return;
            remove();
            if (isAccepted()) log.error("Room ({}) accepted game action failed; cleanup requested", roomId, error);
            result.error(error);
        }

        @Override
        protected void hookOnCancel() {
            // 구독 전 dispose는 BaseSubscriber의 취소 hook을 호출하지 않을 수 있다.
            if (!terminated.compareAndSet(false, true)) return;
            remove();
            result.error(new CancellationException("Game action stopped: " + roomId));
        }
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> executionGate.discardIdle(roomId));
    }

    @Override
    public Mono<Void> withCleanup(long roomId, Supplier<Mono<Void>> operation) {
        return executionGate.withCleanup(roomId, operation);
    }

    @Override
    public Mono<Void> withRestart(long roomId, Supplier<Mono<Void>> operation) {
        return Mono.deferContextual(context -> {
            ActionExecution current = context.getOrDefault(ActionExecution.class, null);
            if (current == null) {
                return Mono.error(new IllegalStateException("Restart requires an active accepted action: " + roomId));
            }
            return current.restart(roomId, operation);
        });
    }
}
