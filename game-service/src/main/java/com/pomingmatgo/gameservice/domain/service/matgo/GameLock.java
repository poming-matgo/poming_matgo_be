package com.pomingmatgo.gameservice.domain.service.matgo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GameLock {
    // 정상 흐름엔 같은 방의 락 경쟁이 없다 — 경쟁이 보였다면 자동플레이 race이므로 대기보다 즉시 실패가 안전
    long waitTime() default 0;
    // -1 = Redisson watchdog 모드 — lease 만료로 인한 동시 진입을 막는다
    long leaseTime() default -1;
}