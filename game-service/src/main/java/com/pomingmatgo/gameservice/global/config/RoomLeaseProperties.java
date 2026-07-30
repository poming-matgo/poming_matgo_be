package com.pomingmatgo.gameservice.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

// duration이 곧 장애 감지 지연(RTO 하한) — 짧을수록 빠른 인수, heartbeat 유실 몇 번에 오탐 인수가 나지 않을 만큼은 길어야 한다
@ConfigurationProperties(prefix = "game.lease")
public record RoomLeaseProperties(
        @DefaultValue("15s") Duration duration,
        @DefaultValue("5s") Duration heartbeatInterval,
        // deadline conflate flush 주기 — 커맨드마다 단건 UPDATE하면 스냅샷 단건 insert와 같은 지배 항이 된다 (§11.6 교훈)
        @DefaultValue("20ms") Duration deadlineFlushInterval
) {
}
