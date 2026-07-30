package com.pomingmatgo.gameservice.global.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

// 저장소 종류(game.lease.store)와 무관하게 항상 바인딩 — RoomLeaseManager/GameRecoveryService는 전 저장소 공통
@Configuration
@EnableConfigurationProperties({RoomLeaseProperties.class, GameRecoveryProperties.class})
public class RoomLeaseConfig {
}
