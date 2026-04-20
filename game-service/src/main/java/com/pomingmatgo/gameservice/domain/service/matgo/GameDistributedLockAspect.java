package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.redisson.api.RLockReactive;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN;

@Profile("redis")
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class GameDistributedLockAspect {

    private final RedissonReactiveClient redissonClient;
    private final ExpressionParser parser = new SpelExpressionParser();

    @Around("@annotation(gameLock)")
    public Mono<Object> lock(ProceedingJoinPoint joinPoint, GameLock gameLock) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Class<?> returnType = signature.getReturnType();

        if (!Mono.class.isAssignableFrom(returnType)) {
            return Mono.error(new IllegalStateException("@GameLock은 Mono를 반환하는 메서드에만 사용할 수 있습니다."));
        }

        String keyName = generateKey(gameLock.key(), joinPoint);
        RLockReactive lock = redissonClient.getLock(keyName);

        long executionId = UUID.randomUUID().getMostSignificantBits();

        return lock.tryLock(gameLock.waitTime(), gameLock.leaseTime(), TimeUnit.MILLISECONDS, executionId)
                .flatMap(available -> {
                    if (!available) {
                        return Mono.error(new WebSocketBusinessException(TRY_AGAIN));
                    }

                    return Mono.usingWhen(
                            Mono.just(lock),
                            l -> {
                                try {
                                    return (Mono<Object>) joinPoint.proceed();
                                } catch (Throwable e) {
                                    return Mono.error(e);
                                }
                            },
                            l -> releaseLock(l, executionId),
                            (l, err) -> releaseLock(l, executionId),
                            l -> releaseLock(l, executionId)
                    );
                });
    }

    private Mono<Void> releaseLock(RLockReactive lock, long executionId) {
        return lock.unlock(executionId)
                .onErrorResume(e -> {
                    if (e instanceof IllegalMonitorStateException) {
                        log.info("Redisson Lock already released (lease time expired): {}", e.getMessage());
                    } else {
                        log.warn("Redisson Lock unlock failed", e);
                    }
                    return Mono.empty();
                });
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