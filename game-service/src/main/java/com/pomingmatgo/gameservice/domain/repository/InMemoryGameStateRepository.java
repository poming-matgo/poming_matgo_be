package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

@Profile("in-memory")
@Repository
public class InMemoryGameStateRepository implements GameStateRepository {

    private final ConcurrentHashMap<Long, GameState> store = new ConcurrentHashMap<>();

    @Override
    public Mono<GameState> findById(long roomId) {
        return Mono.fromCallable(() -> store.get(roomId));
    }

    @Override
    public Mono<Long> create(GameState gameState) {
        return Mono.fromCallable(() -> {
            GameState existing = store.putIfAbsent(gameState.getRoomId(), gameState);
            if (existing != null) {
                throw new BusinessException(ErrorCode.ALREADY_EXISTED_ROOM);
            }
            return gameState.getRoomId();
        });
    }

    @Override
    public Mono<Long> save(GameState gameState) {
        return Mono.fromCallable(() -> {
            // 게임 액션 경로의 save는 RoomLockManager를 타지 않으므로 cleanup(remove)과 동시 실행될 수 있다.
            // 존재 확인과 갱신을 computeIfPresent로 원자화해 삭제된 방이 재삽입(부활)되지 않도록 보장.
            // (Redis 프로파일의 setIfPresent와 동일 계약)
            GameState updated = store.computeIfPresent(gameState.getRoomId(), (k, prev) -> gameState);
            if (updated == null) {
                throw new BusinessException(ErrorCode.SYSTEM_ERROR);
            }
            return gameState.getRoomId();
        });
    }

    @Override
    public Mono<Void> cleanup(long roomId) {
        return Mono.fromRunnable(() -> store.remove(roomId));
    }
}
