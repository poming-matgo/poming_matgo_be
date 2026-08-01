package com.pomingmatgo.gameservice.domain.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

// membership 판정 시계는 전부 DB now() — lease 만료 판정과 같은 시계 (노드 간 wall clock 오차 배제).
// game.log.store=postgres 전제 (같은 DB의 gameLogDatabaseClient 재사용). 별도 gossip/합의 없음 — DB가 진실의 원천
@Component
@ConditionalOnProperty(name = "game.cluster.store", havingValue = "postgres")
@RequiredArgsConstructor
public class PostgresNodeRegistryRepository implements NodeRegistryRepository {

    // active: 배치(링 멤버) 대상 / left: 정상 종료의 최종 상태. draining(살아있되 신규 배치 제외)은 추후 추가
    private static final String ACTIVE = "active";
    private static final String LEFT = "left";

    private final DatabaseClient gameLogDatabaseClient;

    @Override
    public Mono<Void> register(String instanceId, String advertiseAddress) {
        // left 미부활 가드 — leave와 마지막 heartbeat tick의 종료 경합이 유령 멤버를 만들지 못하게 (행 주인은 자기 프로세스뿐)
        DatabaseClient.GenericExecuteSpec spec = gameLogDatabaseClient.sql("""
                        INSERT INTO node_registry (instance_id, state, last_heartbeat, advertise_address)
                        VALUES (:instanceId, :active, now(), :address)
                        ON CONFLICT (instance_id) DO UPDATE SET
                            state = EXCLUDED.state,
                            last_heartbeat = EXCLUDED.last_heartbeat,
                            advertise_address = EXCLUDED.advertise_address
                        WHERE node_registry.state <> :left
                        """)
                .bind("instanceId", instanceId)
                .bind("active", ACTIVE)
                .bind("left", LEFT);
        spec = advertiseAddress == null || advertiseAddress.isBlank()
                ? spec.bindNull("address", String.class)
                : spec.bind("address", advertiseAddress);
        return spec.then();
    }

    @Override
    public Mono<Long> heartbeat(String instanceId) {
        // state는 건드리지 않는다. 향후 구현할 draining 노드도 살아있음은 계속 알려야 한다
        return gameLogDatabaseClient.sql("""
                        UPDATE node_registry SET last_heartbeat = now()
                        WHERE instance_id = :instanceId AND state <> :left
                        """)
                .bind("instanceId", instanceId)
                .bind("left", LEFT)
                .fetch().rowsUpdated();
    }

    @Override
    public Mono<Void> leave(String instanceId) {
        return gameLogDatabaseClient.sql("""
                        UPDATE node_registry SET state = :left, last_heartbeat = now()
                        WHERE instance_id = :instanceId
                        """)
                .bind("left", LEFT)
                .bind("instanceId", instanceId)
                .then();
    }

    @Override
    public Flux<ActiveNode> findActiveNodes(Duration ttl) {
        return gameLogDatabaseClient.sql("""
                        SELECT instance_id, advertise_address FROM node_registry
                        WHERE state = :active
                          AND last_heartbeat > now() - make_interval(secs => :ttlSeconds)
                        """)
                .bind("active", ACTIVE)
                .bind("ttlSeconds", ttl.toMillis() / 1000.0)
                .map(row -> new ActiveNode(
                        row.get("instance_id", String.class),
                        row.get("advertise_address", String.class)))
                .all();
    }
}
