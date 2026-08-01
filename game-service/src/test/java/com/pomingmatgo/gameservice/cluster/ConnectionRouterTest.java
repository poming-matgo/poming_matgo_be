package com.pomingmatgo.gameservice.cluster;

import com.pomingmatgo.gameservice.domain.cluster.ClusterRing;
import com.pomingmatgo.gameservice.domain.cluster.ConnectionRouter;
import com.pomingmatgo.gameservice.domain.cluster.NodeIdentity;
import com.pomingmatgo.gameservice.domain.cluster.NodeRegistry;
import com.pomingmatgo.gameservice.domain.cluster.RoutingDecision;
import com.pomingmatgo.gameservice.domain.repository.NodeRegistryRepository;
import com.pomingmatgo.gameservice.domain.repository.RoomLeaseRepository;
import com.pomingmatgo.gameservice.global.config.NodeRegistryProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionRouterTest {

    private static final NodeRegistryProperties PROPS =
            new NodeRegistryProperties(Duration.ofSeconds(15), Duration.ofSeconds(5), 128, "");
    private static final String NODE_A = "node-a";
    private static final String NODE_B = "node-b";

    private final Map<String, String> addresses = new HashMap<>();
    private final Map<Long, String> leaseOwners = new HashMap<>();
    private boolean clusterEnabled = true;
    private boolean leaseQueried;

    private NodeIdentity identity;
    private ClusterRing clusterRing;
    private ConnectionRouter router;

    @BeforeEach
    void setUp() {
        identity = new NodeIdentity();
        addresses.put(NODE_A, "host-a:8084");
        addresses.put(NODE_B, "host-b:8084");
        clusterRing = new ClusterRing(new NodeRegistry(fakeRegistry(), PROPS, identity), PROPS);
        router = new ConnectionRouter(clusterRing, fakeLeases(), identity);
    }

    private NodeRegistryRepository fakeRegistry() {
        return new NodeRegistryRepository() {
            @Override
            public Mono<Void> register(String instanceId, String advertiseAddress) {
                return Mono.empty();
            }

            @Override
            public Mono<Long> heartbeat(String instanceId) {
                return Mono.just(1L);
            }

            @Override
            public Mono<Void> leave(String instanceId) {
                return Mono.empty();
            }

            @Override
            public Flux<ActiveNode> findActiveNodes(Duration ttl) {
                return Flux.fromIterable(List.of(identity.id(), NODE_A, NODE_B))
                        .map(id -> new ActiveNode(id, addresses.get(id)));
            }

            @Override
            public boolean enabled() {
                return clusterEnabled;
            }
        };
    }

    private RoomLeaseRepository fakeLeases() {
        return new RoomLeaseRepository() {
            @Override
            public Mono<Long> acquire(long roomId, String owner, Duration duration) {
                return Mono.empty();
            }

            @Override
            public Mono<Long> heartbeat(String owner, Duration duration) {
                return Mono.empty();
            }

            @Override
            public Mono<Long> currentToken(long roomId) {
                return Mono.empty();
            }

            @Override
            public Mono<String> findActiveOwner(long roomId) {
                leaseQueried = true;
                return Mono.justOrEmpty(leaseOwners.get(roomId));
            }

            @Override
            public Mono<Void> release(long roomId, long fencingToken) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> recordDeadlines(List<RoomDeadline> batch) {
                return Mono.empty();
            }

            @Override
            public Flux<Long> findExpiredRoomIds() {
                return Flux.empty();
            }

            @Override
            public Mono<Takeover> takeover(long roomId, String newOwner, Duration duration) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> abandon(long roomId, long fencingToken) {
                return Mono.empty();
            }
        };
    }

    private long roomPlannedTo(String nodeId) {
        for (long roomId = 0; roomId < 100_000; roomId++) {
            if (nodeId.equals(clusterRing.plannedNode(roomId).orElse(null))) {
                return roomId;
            }
        }
        throw new IllegalStateException("계획 배치가 " + nodeId + "인 방을 찾지 못함");
    }

    @Test
    @DisplayName("cluster store가 noop이면 항상 로컬 — lease 조회조차 없다 (기본 구성 무비용 통과)")
    void disabledClusterIsAlwaysLocalWithoutLeaseQuery() {
        clusterEnabled = false;

        RoutingDecision decision = router.route(1L).block();
        assertTrue(decision.isLocal());
        assertFalse(leaseQueried);
    }

    @Test
    @DisplayName("유효 lease 소유자가 곧 라우팅 권위 — 링이 다른 노드를 가리켜도 소유자의 주소로 리다이렉트")
    void leaseOwnerBeatsRingHint() {
        clusterRing.refresh().block();
        long roomId = roomPlannedTo(NODE_A);
        leaseOwners.put(roomId, NODE_B);

        RoutingDecision decision = router.route(roomId).block();
        assertEquals("host-b:8084", decision.redirectAddress());
    }

    @Test
    @DisplayName("소유자가 자기 자신이면 로컬 — 복구 진행 중 창은 클라 재시도가 흡수")
    void selfOwnedRoomIsLocal() {
        clusterRing.refresh().block();
        long roomId = roomPlannedTo(NODE_A);
        leaseOwners.put(roomId, identity.id());

        assertTrue(router.route(roomId).block().isLocal());
    }

    @Test
    @DisplayName("소유자의 주소를 모르면(죽은 노드) 리다이렉트 불가 — 로컬 폴백, 인수 완료 후에야 갈 곳이 생긴다")
    void unknownOwnerAddressFallsBackToLocal() {
        clusterRing.refresh().block();
        long roomId = roomPlannedTo(NODE_A);
        leaseOwners.put(roomId, "dead-node");

        assertTrue(router.route(roomId).block().isLocal());
    }

    @Test
    @DisplayName("미소유 방은 링 힌트 — 계획 노드가 로컬이면 로컬, 원격이면 그 노드 주소로 리다이렉트")
    void unownedRoomFollowsRingHint() {
        clusterRing.refresh().block();

        long localRoom = roomPlannedTo(identity.id());
        assertTrue(router.route(localRoom).block().isLocal());

        long remoteRoom = roomPlannedTo(NODE_A);
        assertEquals("host-a:8084", router.route(remoteRoom).block().redirectAddress());
    }

    @Test
    @DisplayName("계획 노드가 주소를 광고하지 않았으면 로컬 폴백 — 링 미형성(기동 직후)도 전 방 로컬")
    void ringHintWithoutAddressFallsBackToLocal() {
        assertTrue(router.route(7L).block().isLocal());

        clusterRing.refresh().block();
        long remoteRoom = roomPlannedTo(NODE_A);
        addresses.remove(NODE_A);
        clusterRing.refresh().block();
        assertTrue(router.route(remoteRoom).block().isLocal());
    }
}
