package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@ConditionalOnProperty(name = "game.log.store", havingValue = "noop", matchIfMissing = true)
public class NoOpGameSnapshotRepository implements GameSnapshotRepository {

    @Override
    public Mono<Void> save(GameSnapshot snapshot) {
        return Mono.empty();
    }

    @Override
    public Mono<GameSnapshot> findLatest(long roomId) {
        return Mono.empty();
    }

    @Override
    public boolean enabled() {
        return false;
    }
}
