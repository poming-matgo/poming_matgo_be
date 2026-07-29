package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import reactor.core.publisher.Mono;

// durability 직교 축 — 로그와 같은 game.log.store 속성으로 선택한다 (스냅샷은 로그 없이 무의미)
public interface GameSnapshotRepository {
    /** 저장 실패·유실은 correctness 문제가 아니다 — 복구 replay가 더 길어질 뿐 */
    Mono<Void> save(GameSnapshot snapshot);

    Mono<GameSnapshot> findLatest(long roomId);

    /** false면 스냅샷 경로 전체를 무비용 통과 (no-op 기본값 — 부하 기준선 직교성) */
    default boolean enabled() {
        return true;
    }
}
