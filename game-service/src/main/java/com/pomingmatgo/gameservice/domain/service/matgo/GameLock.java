package com.pomingmatgo.gameservice.domain.service.matgo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GameLock {
    String key();
    // 정상 게임 흐름에서 같은 (round:turn) 락 경쟁은 발생하지 않음. 경쟁이 보였다면 버그이므로 즉시 실패가 안전 (InMemory 정책과 통일)
    long waitTime() default 0;
    // -1: Redisson watchdog 모드 — 호출 인스턴스 생존 동안 lease 자동 연장 (default 30s, 10s 주기로 갱신). leaseTime 만료로 인한 동시 진입 차단
    long leaseTime() default -1;
}