package com.pomingmatgo.gameservice.domain.repository;

import reactor.core.publisher.Mono;

import java.time.Duration;

// 소유권 직교 축 — 프로파일이 아니라 game.lease.store 속성으로 선택한다. 배타성의 권위는 lease + fencing token뿐이다 (링/캐시가 아니라)
public interface RoomLeaseRepository {

    /** 성공 시 fencing token(방마다 단조 증가). 다른 인스턴스의 유효 lease가 있으면 empty */
    Mono<Long> acquire(long roomId, String owner, Duration duration);

    /** 소유 중인 유효 lease 전부 연장. 만료된 lease는 되살리지 않는다 — 인수(takeover)와의 경합 창 차단 */
    Mono<Long> heartbeat(String owner, Duration duration);

    /** 소유권 상실 판정용 현재 token. lease 행이 없으면 empty */
    Mono<Long> currentToken(long roomId);

    /** 즉시 만료 처리 — 행은 남겨 fencing token 단조 증가를 보존한다. token 불일치면 no-op */
    Mono<Void> release(long roomId, long fencingToken);

    /** false면 lease 경로 전체를 무비용 통과 — no-op 기본값에서 기존 수치가 재현돼야 한다(직교성) */
    default boolean enabled() {
        return true;
    }
}
