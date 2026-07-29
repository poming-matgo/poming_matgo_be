package com.pomingmatgo.gameservice.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

// 배치 크기/주기가 곧 유실 창 — durability 곡선(1-D)의 측정 변수라 속성으로 노출한다
@ConfigurationProperties(prefix = "game.log.batch")
public record GameLogBatchProperties(
        @DefaultValue("64") int maxSize,
        @DefaultValue("20ms") Duration flushInterval
) {
}
