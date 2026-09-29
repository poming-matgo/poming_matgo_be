package com.pomingmatgo.gameservice.infrastructure.lock;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GameLock {
    // 같은 방의 상태 변경 경합은 대기 없이 거부한다.
    long waitTime() default 0;
    // -1 = Redisson watchdog 모드 — lease 만료로 인한 동시 진입을 막는다
    long leaseTime() default -1;
}
