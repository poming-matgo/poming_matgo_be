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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN;

@Profile("in-memory")
@Slf4j
@Aspect
@Component
public class InMemoryGameLockAspect implements GameLockCleaner {

    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, Semaphore>> locksByRoom = new ConcurrentHashMap<>();
    // 파서는 thread-safe하지만 Expression 객체는 evaluation 중 내부 상태를 갱신해 캐싱하면 race가 난다 → 매번 parse
    private final ExpressionParser parser = new SpelExpressionParser();

    @Around("@annotation(gameLock)")
    public Mono<Object> lock(ProceedingJoinPoint joinPoint, GameLock gameLock) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        if (!Mono.class.isAssignableFrom(signature.getReturnType())) {
            return Mono.error(new IllegalStateException("@GameLock은 Mono를 반환하는 메서드에만 사용할 수 있습니다."));
        }

        String keyName = generateKey(gameLock.key(), joinPoint);
        long roomId = extractRoomIdFromArgs(joinPoint);
        Semaphore semaphore = locksByRoom
                .computeIfAbsent(roomId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(keyName, k -> new Semaphore(1));

        // 한 턴엔 한 행위자뿐이라 정상 흐름엔 경쟁이 없다 — 경쟁은 자동플레이 race이므로 즉시 실패
        if (!semaphore.tryAcquire()) {
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
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> locksByRoom.remove(roomId));
    }

    private long extractRoomIdFromArgs(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String[] paramNames = signature.getParameterNames();
        Object[] args = joinPoint.getArgs();
        for (int i = 0; i < paramNames.length; i++) {
            if ("roomId".equals(paramNames[i])) {
                return (Long) args[i];
            }
        }
        throw new IllegalStateException("@GameLock 메서드에 roomId 파라미터가 없습니다: " + signature.getName());
    }

    private String generateKey(String key, ProceedingJoinPoint joinPoint) {
        return "LOCK:" + parser.parseExpression(key)
                .getValue(buildContext(joinPoint), String.class);
    }

    private StandardEvaluationContext buildContext(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        StandardEvaluationContext context = new StandardEvaluationContext();
        Object[] args = joinPoint.getArgs();
        String[] paramNames = signature.getParameterNames();
        if (paramNames != null) {
            for (int i = 0; i < args.length; i++) {
                context.setVariable(paramNames[i], args[i]);
            }
        }
        return context;
    }
}
