package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.GameState;
import reactor.core.publisher.Mono;

public interface GameStateRepository {
    Mono<GameState> findById(long roomId);
    Mono<Long> create(GameState gameState);
    Mono<Long> delete(long roomId);
    Mono<Long> save(GameState gameState);
}
