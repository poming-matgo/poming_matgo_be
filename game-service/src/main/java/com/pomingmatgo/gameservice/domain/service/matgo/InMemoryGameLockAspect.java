package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN;

@Profile("in-memory")
@Slf4j
@Aspect
@Component
public class InMemoryGameLockAspect implements GameLockCleaner {

    private final ConcurrentHashMap<Long, Semaphore> locksByRoom = new ConcurrentHashMap<>();

    @Around("@annotation(gameLock)")
    public Mono<Object> lock(ProceedingJoinPoint joinPoint, GameLock gameLock) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        if (!Mono.class.isAssignableFrom(signature.getReturnType())) {
            return Mono.error(new IllegalStateException("@GameLock은 Mono를 반환하는 메서드에만 사용할 수 있습니다."));
        }

        long roomId = GameLockKey.roomId(joinPoint);

        return Mono.usingWhen(
                Mono.fromSupplier(() -> {
                    Semaphore semaphore = locksByRoom.computeIfAbsent(roomId, k -> new Semaphore(1));
                    // 매 구독마다 획득하며, 경합 시 대기 없이 실패한다.
                    if (!semaphore.tryAcquire()) {
                        throw new WebSocketBusinessException(TRY_AGAIN);
                    }
                    return semaphore;
                }),
                s -> {
                    try {
                        return (Mono<Object>) joinPoint.proceed();
                    } catch (Throwable e) {
                        return Mono.error(e);
                    }
                },
                s -> Mono.fromRunnable(s::release),
                (s, err) -> Mono.fromRunnable(s::release),
                s -> Mono.fromRunnable(s::release)
        );
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> locksByRoom.remove(roomId));
    }
}
