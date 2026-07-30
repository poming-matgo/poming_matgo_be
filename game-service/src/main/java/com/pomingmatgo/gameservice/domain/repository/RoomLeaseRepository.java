package com.pomingmatgo.gameservice.domain.repository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

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

    /** 크로스 프로세스 타이머 복원값. token 불일치 방의 행은 조용히 빠진다 — 유실 = 인수 시 즉시 자동플레이일 뿐 */
    Mono<Void> recordDeadlines(List<RoomDeadline> batch);

    record RoomDeadline(long roomId, long fencingToken, long deadlineEpochMillis) {}

    /** 인수 후보 = 만료됐지만 정상 해제(released)는 아닌 방 — 해제는 cleanup 완주의 증거라 인수 대상이 아니다 */
    Flux<Long> findExpiredRoomIds();

    /** 만료 lease 한정 원자적 인수 — 승자 1명만 token+1을 받는다. 유효 lease거나 경쟁 패배면 empty */
    Mono<Takeover> takeover(long roomId, String newOwner, Duration duration);

    record Takeover(long fencingToken, Long turnDeadlineEpochMillis) {}

    /** 복구 실패 시 즉시 만료 — release와 달리 owner를 남겨 재시도(재인수) 대상으로 되돌린다. token 불일치면 no-op */
    Mono<Void> abandon(long roomId, long fencingToken);

    /** false면 lease 경로 전체를 무비용 통과 — no-op 기본값에서 기존 수치가 재현돼야 한다(직교성) */
    default boolean enabled() {
        return true;
    }
}
