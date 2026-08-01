package com.pomingmatgo.gameservice.domain.repository;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@ConditionalOnProperty(name = "game.cluster.store", havingValue = "noop", matchIfMissing = true)
public class NoOpNodeRegistryRepository implements NodeRegistryRepository {

    @Override
    public Mono<Void> register(String instanceId) {
        return Mono.empty();
    }

    @Override
    public Mono<Long> heartbeat(String instanceId) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> leave(String instanceId) {
        return Mono.empty();
    }

    @Override
    public Flux<String> findActiveNodeIds(Duration ttl) {
        return Flux.empty();
    }

    @Override
    public boolean enabled() {
        return false;
    }
}
