package com.pomingmatgo.gameservice.domain.cluster;

import com.pomingmatgo.gameservice.global.config.NodeRegistryProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.pomingmatgo.gameservice.domain.repository.NodeRegistryRepository;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

// 현재 멤버십의 링 스냅샷 — 배치 힌트 전용, 소유 판정에 쓰지 말 것.
// 노드마다 각자 리빌드하지만 HashRing 빌드가 결정적이라 같은 멤버십 = 같은 링 — 노드 간 링 동기화 불필요
@Component
@RequiredArgsConstructor
@Slf4j
public class ClusterRing {

    private final NodeRegistry registry;
    private final NodeRegistryProperties properties;
    private volatile HashRing ring = HashRing.EMPTY;
    // 리다이렉트 대상 주소 — 링과 같은 멤버십 스냅샷에서 갱신 (접속 라우팅에 DB 왕복을 더하지 않는 장치)
    private volatile Map<String, String> addresses = Map.of();
    private Disposable refreshLoop;

    @PostConstruct
    void start() {
        if (!registry.enabled()) {
            return;
        }
        // 멤버십은 heartbeat 주기로만 변하므로 같은 주기로 리빌드 — 반영 지연 상한은 ttl + interval
        refreshLoop = Flux.interval(Duration.ZERO, properties.heartbeatInterval())
                .onBackpressureDrop()
                .concatMap(tick -> refresh()
                        .onErrorResume(e -> {
                            log.warn("링 리빌드 실패 — 직전 스냅샷 유지", e);
                            return Mono.empty();
                        }))
                .subscribe();
    }

    @PreDestroy
    void stop() {
        if (refreshLoop != null) {
            refreshLoop.dispose();
        }
    }

    public Mono<Void> refresh() {
        return registry.activeNodes().collectList().doOnNext(this::rebuild).then();
    }

    private void rebuild(List<NodeRegistryRepository.ActiveNode> members) {
        HashRing next = HashRing.build(
                members.stream().map(NodeRegistryRepository.ActiveNode::instanceId).toList(),
                properties.virtualNodes());
        if (!next.nodes().equals(ring.nodes())) {
            log.info("링 멤버십 변경 — {} -> {}", ring.nodes(), next.nodes());
        }
        addresses = members.stream()
                .filter(node -> node.address() != null && !node.address().isBlank())
                .collect(Collectors.toUnmodifiableMap(
                        NodeRegistryRepository.ActiveNode::instanceId,
                        NodeRegistryRepository.ActiveNode::address));
        ring = next;
    }

    /** 배치 힌트 — membership 미사용·기동 직후 미형성이면 empty */
    public Optional<String> plannedNode(long roomId) {
        return Optional.ofNullable(ring.route(roomId));
    }

    /** 링이 없으면(단일 노드 구성) 모든 방이 로컬 — noop 기본값에서 라우팅 경로가 무비용 통과하는 지점 */
    public boolean isPlannedLocal(long roomId) {
        String planned = ring.route(roomId);
        return planned == null || planned.equals(registry.instanceId());
    }

    /** 리다이렉트 주소 — 죽었거나 left거나 주소 미광고 노드는 empty (그 노드로는 리다이렉트 불가) */
    public Optional<String> addressOf(String instanceId) {
        return Optional.ofNullable(addresses.get(instanceId));
    }

    public boolean enabled() {
        return registry.enabled();
    }
}
