package com.pomingmatgo.gameservice.repository;

import com.pomingmatgo.gameservice.domain.repository.InMemoryLeadingPlayerRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("선 선택 트리거 원자성 단위 테스트")
class InMemoryLeadingPlayerRepositoryTest {

    private final InMemoryLeadingPlayerRepository repository = new InMemoryLeadingPlayerRepository();

    @Test
    @DisplayName("두 플레이어가 동시에 선택을 마쳐도 후속 트리거는 정확히 한 번만 claim된다")
    void triggerClaimedExactlyOnceUnderContention() throws Exception {
        long roomId = 1L;
        int threads = 16;
        AtomicInteger claims = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        if (Boolean.TRUE.equals(repository.tryClaimLeaderSelectionTrigger(roomId).block())) {
                            claims.incrementAndGet();
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

        assertEquals(1, claims.get(), "게임 시작 트리거는 1회만 발사돼야 한다");
    }

    @Test
    @DisplayName("cleanup 후에는 트리거를 다시 claim할 수 있다 (다음 게임 재사용)")
    void triggerReclaimableAfterCleanup() {
        long roomId = 2L;
        assertTrue(repository.tryClaimLeaderSelectionTrigger(roomId).block());
        assertFalse(repository.tryClaimLeaderSelectionTrigger(roomId).block());

        repository.cleanup(roomId).block();

        assertTrue(repository.tryClaimLeaderSelectionTrigger(roomId).block());
    }
}
