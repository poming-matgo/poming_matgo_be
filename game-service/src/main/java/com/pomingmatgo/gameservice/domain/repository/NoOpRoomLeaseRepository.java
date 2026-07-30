package com.pomingmatgo.gameservice.domain.repository;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

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

    @Override
    public boolean enabled() {
        return false;
    }
}
