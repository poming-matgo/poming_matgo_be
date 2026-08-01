package com.pomingmatgo.gameservice.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

// ttl이 곧 죽은 노드의 멤버십 이탈 지연 — lease duration과 같은 절충(heartbeat 몇 번 유실에 오탐 이탈이 나지 않을 만큼)
@ConfigurationProperties(prefix = "game.cluster")
public record NodeRegistryProperties(
        @DefaultValue("15s") Duration ttl,
        @DefaultValue("5s") Duration heartbeatInterval
) {
}
