package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.global.lock.InMemoryRoomExecutionGate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

@Profile("in-memory")
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class InMemoryGameLockAspect implements GameLockCleaner {

    private final InMemoryRoomExecutionGate executionGate;

    @Around("@annotation(gameLock)")
    public Mono<Object> lock(ProceedingJoinPoint joinPoint, GameLock gameLock) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        if (!Mono.class.isAssignableFrom(signature.getReturnType())) {
            return Mono.error(new IllegalStateException("@GameLock은 Mono를 반환하는 메서드에만 사용할 수 있습니다."));
        }

        long roomId = GameLockKey.roomId(joinPoint);

        return Mono.usingWhen(
                Mono.fromSupplier(() -> executionGate.acquire(roomId)),
                s -> {
                    try {
                        return (Mono<Object>) joinPoint.proceed();
                    } catch (Throwable e) {
                        return Mono.error(e);
                    }
                },
                s -> Mono.fromRunnable(() -> executionGate.release(s)),
                (s, err) -> Mono.fromRunnable(() -> executionGate.release(s)),
                s -> Mono.fromRunnable(() -> executionGate.release(s))
        );
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
