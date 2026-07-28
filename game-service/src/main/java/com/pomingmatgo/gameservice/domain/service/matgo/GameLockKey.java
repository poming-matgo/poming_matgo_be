package com.pomingmatgo.gameservice.domain.service.matgo;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;

// 게임 액션 락은 방 단위 키 하나뿐이라 표현식이 필요 없다 — 한 턴엔 한 행위자뿐이라 액션 종류로 나눠도 창구만 생긴다
final class GameLockKey {

    private GameLockKey() {}

    static long roomId(ProceedingJoinPoint joinPoint) {
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

    static String lockName(long roomId) {
        return "LOCK:game:" + roomId;
    }
}
