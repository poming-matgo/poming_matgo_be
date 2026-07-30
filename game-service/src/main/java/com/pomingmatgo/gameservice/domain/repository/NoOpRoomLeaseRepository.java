package com.pomingmatgo.gameservice.domain.repository;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@ConditionalOnProperty(name = "game.lease.store", havingValue = "noop", matchIfMissing = true)
public class NoOpRoomLeaseRepository implements RoomLeaseRepository {

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
    public Mono<Void> release(long roomId, long fencingToken) {
        return Mono.empty();
    }

    @Override
    public boolean enabled() {
        return false;
    }
}
