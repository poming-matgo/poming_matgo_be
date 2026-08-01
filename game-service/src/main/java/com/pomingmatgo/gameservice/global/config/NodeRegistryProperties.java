package com.pomingmatgo.gameservice.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

// ttl이 곧 죽은 노드의 멤버십 이탈 지연 — lease duration과 같은 절충(heartbeat 몇 번 유실에 오탐 이탈이 나지 않을 만큼)
@ConfigurationProperties(prefix = "game.cluster")
public record NodeRegistryProperties(
        @DefaultValue("15s") Duration ttl,
        @DefaultValue("5s") Duration heartbeatInterval,
        // 128 = 분포 편차 측정 결과(HashRingTest)로 선택 — 편차 개선이 체감 둔화되는 지점.
        @DefaultValue("128") int virtualNodes,
        // 클라 리다이렉트용 광고 주소(host:port) — 서버는 해석하지 않는 불투명 문자열. 미설정이면 타 노드가 이 노드로 리다이렉트 못한다
        @DefaultValue("") String advertiseAddress
) {
}
