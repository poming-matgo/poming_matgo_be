package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.annotation.Profile;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN;

@Profile("local")
@Slf4j
@Aspect
@Component
public class InMemoryGameLockAspect implements GameLockCleaner {

    private final ConcurrentHashMap<String, Semaphore> locks = new ConcurrentHashMap<>();
    private final ExpressionParser parser = new SpelExpressionParser();

    @Around("@annotation(gameLock)")
    public Mono<Object> lock(ProceedingJoinPoint joinPoint, GameLock gameLock) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        if (!Mono.class.isAssignableFrom(signature.getReturnType())) {
            return Mono.error(new IllegalStateException("@GameLock은 Mono를 반환하는 메서드에만 사용할 수 있습니다."));
        }

        String keyName = generateKey(gameLock.key(), joinPoint);
        Semaphore semaphore = locks.computeIfAbsent(keyName, k -> new Semaphore(1));

        // tryAcquire는 블로킹이므로 boundedElastic에서 실행
        return Mono.fromCallable(() -> semaphore.tryAcquire(gameLock.waitTime(), TimeUnit.MILLISECONDS))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(acquired -> {
                    if (!acquired) {
                        return Mono.error(new WebSocketBusinessException(TRY_AGAIN));
                    }
                    return Mono.usingWhen(
                            Mono.just(semaphore),
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
                });
    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> locksByRoom.remove(roomId));
    }

    private String generateKey(String key, ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        StandardEvaluationContext context = new StandardEvaluationContext();
        Object[] args = joinPoint.getArgs();
        String[] paramNames = signature.getParameterNames();

        if (paramNames != null) {
            for (int i = 0; i < args.length; i++) {
                context.setVariable(paramNames[i], args[i]);
            }
        }
        return "LOCK:" + parser.parseExpression(key).getValue(context, String.class);
    }
}
