package com.pomingmatgo.gameservice.domain.service.matgo;

import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Profile("in-memory")
@Aspect
@Component
@RequiredArgsConstructor
public class InMemoryGameLockAspect {

    private final InMemoryGameActionExecutor executor;

    @Around("@annotation(gameLock)")
    public Mono<Object> lock(ProceedingJoinPoint joinPoint, GameLock gameLock) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        if (!Mono.class.isAssignableFrom(signature.getReturnType())) {
            return Mono.error(new IllegalStateException("@GameLock은 Mono를 반환하는 메서드에만 사용할 수 있습니다."));
        }

        long roomId = GameLockKey.roomId(joinPoint);
        return executor.execute(roomId, () -> {
            try {
                return (Mono<Object>) joinPoint.proceed();
            } catch (Throwable error) {
                return Mono.error(error);
            }
        });
    }
}
