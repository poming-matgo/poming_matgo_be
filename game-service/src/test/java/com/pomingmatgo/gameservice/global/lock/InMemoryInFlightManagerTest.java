package com.pomingmatgo.gameservice.global.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("InFlight 플래그 동시성 단위 테스트")
class InMemoryInFlightManagerTest {

    private static final String KEY = InFlightManager.normalKey(1L, 1);
    private static final Duration TTL = Duration.ofSeconds(5);

    private InMemoryInFlightManager manager;

    @BeforeEach
    void setUp() {
        manager = new InMemoryInFlightManager();
    }

    @Test
    @DisplayName("같은 키의 동시 trySetFlag는 정확히 하나만 성공한다")
    void onlyOneWinnerUnderContention() throws Exception {
        AtomicInteger successes = new AtomicInteger();

        runConcurrently(32, i -> {
            if (Boolean.TRUE.equals(manager.trySetFlag(KEY, "token-" + i, TTL).block())) {
                successes.incrementAndGet();
            }
        });

        assertEquals(1, successes.get());
        assertTrue(manager.isSet(KEY).block());
    }

    @Test
    @DisplayName("TTL 내에는 재획득이 실패하고, 만료 후에는 성공한다")
    void reacquireOnlyAfterExpiry() throws Exception {
        assertTrue(manager.trySetFlag(KEY, "first", Duration.ofMillis(80)).block());
        assertFalse(manager.trySetFlag(KEY, "second", TTL).block(), "TTL 내 재획득은 거부돼야 한다");

        Thread.sleep(150);

        assertTrue(manager.trySetFlag(KEY, "second", TTL).block(), "만료된 플래그는 새 요청이 획득할 수 있어야 한다");
    }

    @Test
    @DisplayName("만료된 잔존 엔트리를 두고 경쟁해도 정확히 하나만 교체에 성공한다")
    void expiredEntryReplacedByExactlyOneWinner() throws Exception {
        assertTrue(manager.trySetFlag(KEY, "stale", Duration.ofMillis(1)).block());
        Thread.sleep(50);

        AtomicInteger successes = new AtomicInteger();
        runConcurrently(32, i -> {
            if (Boolean.TRUE.equals(manager.trySetFlag(KEY, "token-" + i, TTL).block())) {
                successes.incrementAndGet();
            }
        });

        assertEquals(1, successes.get());
    }

    @Test
    @DisplayName("deleteFlag는 소유 토큰이 일치할 때만 삭제한다")
    void deleteRequiresOwnership() {
        manager.trySetFlag(KEY, "owner", TTL).block();

        manager.deleteFlag(KEY, "not-owner").block();
        assertTrue(manager.isSet(KEY).block(), "다른 토큰의 삭제 요청은 무시돼야 한다");

        manager.deleteFlag(KEY, "owner").block();
        assertFalse(manager.isSet(KEY).block());
    }

    @Test
    @DisplayName("TTL 만료 후 재획득된 플래그를 낡은 소유자의 정리가 지우지 못한다")
    void staleOwnerCleanupCannotDeleteNewOwnersFlag() throws Exception {
        manager.trySetFlag(KEY, "stale-owner", Duration.ofMillis(1)).block();
        Thread.sleep(50);
        assertTrue(manager.trySetFlag(KEY, "new-owner", TTL).block());

        // 뒤늦게 끝난 원 소유자의 정리 호출
        manager.deleteFlag(KEY, "stale-owner").block();

        assertTrue(manager.isSet(KEY).block(), "새 소유자의 플래그가 남아 있어야 한다");
    }

    @Test
    @DisplayName("만료된 플래그는 isSet에서 false다")
    void expiredFlagIsNotSet() throws Exception {
        manager.trySetFlag(KEY, "owner", Duration.ofMillis(1)).block();
        Thread.sleep(50);

        assertFalse(manager.isSet(KEY).block());
    }

    private void runConcurrently(int threads, IntConsumer task) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int i = 0; i < threads; i++) {
                int idx = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        task.accept(idx);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS), "동시 실행이 제한 시간 내에 끝나야 한다");
        } finally {
            pool.shutdownNow();
        }
    }
}
