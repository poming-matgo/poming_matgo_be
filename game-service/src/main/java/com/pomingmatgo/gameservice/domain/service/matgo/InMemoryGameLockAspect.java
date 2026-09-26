package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.global.lock.InMemoryRoomExecutionGate;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.MonoSink;
import reactor.util.context.Context;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

@Profile("in-memory")
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class InMemoryGameLockAspect implements GameLockCleaner {

    private final InMemoryRoomExecutionGate executionGate;
    private final Set<ActionExecution> executions = new HashSet<>();
    private boolean stopped;

    @Around("@annotation(gameLock)")
    public Mono<Object> lock(ProceedingJoinPoint joinPoint, GameLock gameLock) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        if (!Mono.class.isAssignableFrom(signature.getReturnType())) {
            return Mono.error(new IllegalStateException("@GameLock은 Mono를 반환하는 메서드에만 사용할 수 있습니다."));
        }

        long roomId = GameLockKey.roomId(joinPoint);

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
            locked(joinPoint, roomId, execution).subscribe(execution);
        });
    }

    private Mono<Object> locked(ProceedingJoinPoint joinPoint, long roomId, ActionExecution execution) {
        return Mono.usingWhen(
                Mono.fromSupplier(() -> executionGate.acquire(roomId)),
                s -> Mono.defer(() -> {
                    try {
                        return (Mono<Object>) joinPoint.proceed();
                    } catch (Throwable e) {
                        return Mono.error(e);
                    }
                }).contextWrite(context -> context.put(GameActionAcceptance.class,
                        (BooleanSupplier) () -> execution.accept(s)))
                        .doOnError(error -> {
                            if (execution.isAccepted()) executionGate.fail(s);
                        }),
                s -> Mono.fromRunnable(() -> executionGate.release(s)),
                (s, err) -> Mono.fromRunnable(() -> executionGate.release(s)),
                s -> Mono.fromRunnable(() -> executionGate.release(s))
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
                accepted = true;
            }
            return true;
        }

        private synchronized boolean isAccepted() {
            return accepted;
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
            if (isAccepted()) log.error("Room ({}) accepted game action failed; blocked until cleanup", roomId, error);
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
}
