package com.pomingmatgo.gameservice.repository;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.InMemoryGameStateRepository;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("게임 상태 인메모리 저장소 동시성 단위 테스트")
class InMemoryGameStateRepositoryTest {

    private final InMemoryGameStateRepository repository = new InMemoryGameStateRepository();

    @Test
    @DisplayName("같은 방의 동시 create는 정확히 하나만 성공한다")
    void concurrentCreateHasSingleWinner() throws Exception {
        long roomId = 1L;
        int threads = 16;
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        repository.create(GameState.createEmptyRoom(roomId)).block();
                        successes.incrementAndGet();
                    } catch (BusinessException e) {
                        if (e.getErrorCode() == ErrorCode.ALREADY_EXISTED_ROOM) {
                            duplicates.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, successes.get());
        assertEquals(threads - 1, duplicates.get());
    }

    @Test
    @DisplayName("cleanup된 방에 대한 save는 실패한다 (삭제된 방 부활 금지)")
    void saveCannotResurrectCleanedUpRoom() {
        long roomId = 2L;
        repository.create(GameState.createEmptyRoom(roomId)).block();
        repository.cleanup(roomId).block();

        GameState late = GameState.createEmptyRoom(roomId).toBuilder()
                .phase(GamePhase.IN_PROGRESS)
                .build();
        BusinessException e = assertThrows(BusinessException.class, () -> repository.save(late).block());
        assertEquals(ErrorCode.SYSTEM_ERROR, e.getErrorCode());
        assertNull(repository.findById(roomId).block(), "삭제된 방이 재삽입되면 안 된다");
    }

    @Test
    @DisplayName("존재하는 방의 save는 상태를 갱신한다")
    void saveUpdatesExistingRoom() {
        long roomId = 3L;
        repository.create(GameState.createEmptyRoom(roomId)).block();

        GameState updated = GameState.createEmptyRoom(roomId).toBuilder()
                .phase(GamePhase.IN_PROGRESS).round(1).currentTurn(1)
                .build();
        repository.save(updated).block();

        assertEquals(GamePhase.IN_PROGRESS, repository.findById(roomId).block().getPhase());
    }
}
