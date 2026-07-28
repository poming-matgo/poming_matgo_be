package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@ConditionalOnProperty(name = "game.log.store", havingValue = "noop", matchIfMissing = true)
public class NoOpGameLogRepository implements GameLogRepository {

    @Override
    public Mono<Void> append(long roomId, List<GameLogRecord> batch) {
        return Mono.empty();
    }

    @Override
    public Flux<GameLogRecord> findAllFromSeq(long roomId, long fromSeq) {
        return Flux.empty();
    }

    @Override
    public Mono<Void> markCompleted(long roomId) {
        return Mono.empty();
    }

    @Override
    public boolean enabled() {
        return false;
    }
}
