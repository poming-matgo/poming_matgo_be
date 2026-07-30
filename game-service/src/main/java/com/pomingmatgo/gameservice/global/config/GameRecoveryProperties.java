package com.pomingmatgo.gameservice.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

// scan-interval이 RTO의 두 번째 항이다 (lease duration = 감지 지연 + scan = 인수 지연). 0 이하면 스캔 루프 비활성(수동 scanOnce만)
@ConfigurationProperties(prefix = "game.recovery")
public record GameRecoveryProperties(
        @DefaultValue("5s") Duration scanInterval
) {
}
