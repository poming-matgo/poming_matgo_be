package com.pomingmatgo.gameservice.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// game.log.store=postgres일 때만 바인딩된다 — Redis(핫 상태 공유)와 역할 분리된 durable 로그 전용 접속
@ConfigurationProperties(prefix = "game.log.postgres")
public record GameLogPostgresProperties(
        String url,
        String username,
        String password,
        @DefaultValue("2") int poolInitialSize,
        @DefaultValue("10") int poolMaxSize,
        @DefaultValue("true") boolean initSchema
) {
}
