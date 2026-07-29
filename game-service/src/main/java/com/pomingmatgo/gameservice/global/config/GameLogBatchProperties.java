package com.pomingmatgo.gameservice.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

// 배치 크기/주기가 곧 유실 창 — durability 곡선(1-D)의 측정 변수라 속성으로 노출한다
@ConfigurationProperties(prefix = "game.log.batch")
public record GameLogBatchProperties(
        @DefaultValue("64") int maxSize,
        @DefaultValue("20ms") Duration flushInterval,
        // 방당 cadence(~1.5/s)에선 방 단위 배치≈1(1-D §6.1) — 여러 방을 한 배치로 묶어야 왕복이 준다. 유실 창 모양이 바뀌므로(배치 1건 = 여러 방 × 1건) opt-in
        @DefaultValue("false") boolean crossRoom,
        // 전역 채널 1개는 emit 락 convoy + 직렬 insert 한계로 스톨 (실측) — 방 해시로 분할해 락·writer를 스트라이핑
        @DefaultValue("8") int writerShards,
        // 스냅샷 단건 insert가 durable 커밋의 지배 항(1-E 실측 ~68%) — 여러 방의 스냅샷을 한 multi-row insert로 묶는다. 로그 cross-room과 독립 축(1-D 측정 변수)
        @DefaultValue("false") boolean snapshotCrossRoom
) {
}
