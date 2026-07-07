package com.pomingmatgo.gameservice.domain;

import java.util.concurrent.TimeUnit;

public final class TurnTiming {

    public static final long TURN_TIMEOUT_MILLIS = 10000;
    public static final long GRACE_PERIOD_MILLIS = 2000;

    // 자동플레이 deadline: 클라이언트 턴 제한 + 서버 측 RTT 보정 여유 (monotonic clock 기준)
    public static long nextDeadlineNanos() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TURN_TIMEOUT_MILLIS + GRACE_PERIOD_MILLIS);
    }

    private TurnTiming() {}
}
