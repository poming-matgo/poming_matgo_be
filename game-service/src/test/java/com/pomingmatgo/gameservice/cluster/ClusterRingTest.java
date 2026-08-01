package com.pomingmatgo.gameservice.cluster;

import com.pomingmatgo.gameservice.domain.cluster.ClusterRing;
import com.pomingmatgo.gameservice.domain.cluster.HashRing;
import com.pomingmatgo.gameservice.domain.cluster.NodeIdentity;
import com.pomingmatgo.gameservice.domain.cluster.NodeRegistry;
import com.pomingmatgo.gameservice.domain.repository.NodeRegistryRepository;
import com.pomingmatgo.gameservice.global.config.NodeRegistryProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClusterRingTest {

    private static final NodeRegistryProperties PROPS =
            new NodeRegistryProperties(Duration.ofSeconds(15), Duration.ofSeconds(5), 128);

    private final List<String> activeMembers = new ArrayList<>();
    private NodeIdentity identity;
    private ClusterRing clusterRing;

    // membership만 흉내내는 fake — 링 리빌드가 activeNodes 스냅샷을 그대로 반영하는지가 관심사
    private NodeRegistryRepository fakeRepository() {
        return new NodeRegistryRepository() {
            @Override
            public Mono<Void> register(String instanceId) {
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
            public Flux<String> findActiveNodeIds(Duration ttl) {
                return Flux.fromIterable(List.copyOf(activeMembers));
            }
        };
    }

    @BeforeEach
    void setUp() {
        identity = new NodeIdentity();
        clusterRing = new ClusterRing(new NodeRegistry(fakeRepository(), PROPS, identity), PROPS);
    }

    @Test
    @DisplayName("refresh는 멤버십 스냅샷으로 링을 리빌드 — 배치는 같은 멤버십의 HashRing과 동일(노드 간 링 동기화 불필요의 근거)")
    void refreshBuildsRingFromMembership() {
        activeMembers.addAll(List.of(identity.id(), "node-a", "node-b"));
        clusterRing.refresh().block();

        HashRing expected = HashRing.build(activeMembers, PROPS.virtualNodes());
        for (long roomId = 0; roomId < 5_000; roomId++) {
            assertEquals(expected.route(roomId), clusterRing.plannedNode(roomId).orElseThrow());
            assertEquals(identity.id().equals(expected.route(roomId)), clusterRing.isPlannedLocal(roomId));
        }
    }

    @Test
    @DisplayName("멤버 이탈 후 refresh: 떠난 노드의 방만 재배치되고 나머지 배치는 불변")
    void refreshReflectsMembershipChange() {
        activeMembers.addAll(List.of(identity.id(), "node-a", "node-b"));
        clusterRing.refresh().block();
        List<String> before = new ArrayList<>();
        for (long roomId = 0; roomId < 5_000; roomId++) {
            before.add(clusterRing.plannedNode(roomId).orElseThrow());
        }

        activeMembers.remove("node-a");
        clusterRing.refresh().block();
        for (long roomId = 0; roomId < 5_000; roomId++) {
            String planned = clusterRing.plannedNode(roomId).orElseThrow();
            assertNotEquals("node-a", planned);
            if (!"node-a".equals(before.get((int) roomId))) {
                assertEquals(before.get((int) roomId), planned);
            }
        }
    }

    @Test
    @DisplayName("링 미형성(기동 직후·멤버 0)이면 배치 힌트 없음 + 모든 방이 로컬 — 단일 노드 폴백")
    void emptyRingFallsBackToLocal() {
        assertTrue(clusterRing.plannedNode(1L).isEmpty());
        assertTrue(clusterRing.isPlannedLocal(1L));

        clusterRing.refresh().block();
        assertTrue(clusterRing.plannedNode(1L).isEmpty());
        assertTrue(clusterRing.isPlannedLocal(1L));
    }
}
