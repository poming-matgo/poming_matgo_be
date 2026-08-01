package com.pomingmatgo.gameservice.domain.repository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

// membership 직교 축 — 프로파일이 아니라 game.cluster.store 속성으로 선택한다.
// 레지스트리는 배치 힌트일 뿐 — 배타성의 권위는 여전히 lease + fencing token이다
public interface NodeRegistryRepository {

    /** 멤버십 편입 upsert. left 행은 되살리지 않는다 — instance_id는 프로세스 고유라 재기동은 새 행, 되살리기는 종료 경합의 유령만 만든다 */
    Mono<Void> register(String instanceId);

    /** 자기 행의 last_heartbeat 갱신. left면 0행 — 갱신 시각은 DB now() (lease와 같은 시계) */
    Mono<Long> heartbeat(String instanceId);

    /** 정상 종료 — 행은 남기고 상태 전이만(최종 상태). 죽은 노드는 leave 없이 heartbeat 정체로 멤버십에서 빠진다 */
    Mono<Void> leave(String instanceId);

    /** 살아있는 멤버 = active + last_heartbeat가 ttl 이내 — 판정 시계는 DB now() */
    Flux<String> findActiveNodeIds(Duration ttl);

    /** false면 멤버십 경로 전체를 무비용 통과 — no-op 기본값에서 기존 수치가 재현돼야 한다(직교성) */
    default boolean enabled() {
        return true;
    }
}
