package com.pomingmatgo.gameservice.domain.cluster;

import com.pomingmatgo.gameservice.domain.repository.NodeRegistryRepository;
import com.pomingmatgo.gameservice.global.config.NodeRegistryProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

// 노드 membership — 죽은 노드는 leave 없이 heartbeat 정체(ttl 초과)로 멤버십에서 빠진다.
// 링은 이 목록을 배치 힌트로만 쓴다 — 방 소유의 권위는 어디까지나 lease + fencing token
@Component
@RequiredArgsConstructor
@Slf4j
public class NodeRegistry {

    private final NodeRegistryRepository repository;
    private final NodeRegistryProperties properties;
    private final NodeIdentity identity;
    private Disposable heartbeat;

    @PostConstruct
    void start() {
        if (!repository.enabled()) {
            return;
        }
        // 첫 tick 즉시 발사 = 최초 등록 — 별도 등록 호출 없이 beat의 자기 치유가 겸한다 (기동 시 DB 순단에도 다음 tick이 복구)
        heartbeat = Flux.interval(Duration.ZERO, properties.heartbeatInterval())
                // DB 지연으로 밀린 tick은 버린다 — interval은 backpressure를 못 받아 밀리면 overflow로 루프째 죽는다
                .onBackpressureDrop()
                .concatMap(tick -> beat()
                        .onErrorResume(e -> {
                            log.warn("node heartbeat 실패 — instanceId={}", identity.id(), e);
                            return Mono.empty();
                        }))
                .subscribe();
        log.info("node registry 활성 — instanceId={}, ttl={}, heartbeatInterval={}",
                identity.id(), properties.ttl(), properties.heartbeatInterval());
    }

    @PreDestroy
    void stop() {
        if (heartbeat != null) {
            heartbeat.dispose();
        }
        if (repository.enabled()) {
            try {
                repository.leave(identity.id()).block(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                // leave 실패 = ttl 만료로 자연 이탈할 뿐 — 종료를 막지 않는다
                log.warn("node leave 실패 — instanceId={}", identity.id(), e);
            }
        }
    }

    /** heartbeat 1회 — 행이 없으면 등록(자기 치유). left면 아무 일도 안 일어난다 (register의 미부활 가드) */
    public Mono<Void> beat() {
        return repository.heartbeat(identity.id())
                .filter(rows -> rows == 0)
                .flatMap(rows -> repository.register(identity.id()))
                .then();
    }

    /** 링의 멤버 목록 — 판정 시계는 DB now() */
    public Flux<String> activeNodes() {
        return repository.findActiveNodeIds(properties.ttl());
    }

    public String instanceId() {
        return identity.id();
    }

    public boolean enabled() {
        return repository.enabled();
    }
}
