package com.pomingmatgo.gameservice.global.lock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("방 단위 락 동시성 단위 테스트")
class InMemoryRoomLockManagerTest {

    private final InMemoryRoomLockManager lockManager = new InMemoryRoomLockManager();

    @Test
    @DisplayName("같은 방의 임계 구역은 동시에 하나만 실행된다")
    void mutualExclusionWithinRoom() throws Exception {
        int tasks = 20;
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxActive = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        CountDownLatch done = new CountDownLatch(tasks);

        for (int i = 0; i < tasks; i++) {
            lockManager.withLock(100L, Mono.fromCallable(() -> {
                        int current = active.incrementAndGet();
                        maxActive.accumulateAndGet(current, Math::max);
                        Thread.sleep(5);
                        active.decrementAndGet();
                        return completed.incrementAndGet();
                    }), () -> new IllegalStateException("lock fail"))
                    .subscribe(v -> done.countDown(), e -> {
                        failures.add(e);
                        done.countDown();
                    });
        }

        assertTrue(done.await(5, TimeUnit.SECONDS), "모든 작업이 제한 시간 내에 끝나야 한다");
        assertTrue(failures.isEmpty(), "락 획득 실패가 없어야 한다: " + failures);
        assertEquals(tasks, completed.get());
        assertEquals(1, maxActive.get(), "임계 구역 동시 실행은 1을 넘을 수 없다");
    }

    @Test
    @DisplayName("점유가 길어지면 대기자는 타임아웃 후 lockFailError를 받는다")
    void waiterFailsAfterTimeout() throws Exception {
        CountDownLatch acquired = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        lockManager.withLock(200L, Mono.fromCallable(() -> {
                    acquired.countDown();
                    release.await(10, TimeUnit.SECONDS);
                    return 1;
                }), () -> new IllegalStateException("holder fail"))
                .subscribe();
        assertTrue(acquired.await(2, TimeUnit.SECONDS));

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> lockManager.withLock(200L, Mono.just(1), () -> new IllegalStateException("BUSY")).block());
        assertEquals("BUSY", error.getMessage());

        // 점유 해제 후에는 다시 획득 가능 (정상 완료 경로의 release 검증)
        release.countDown();
        assertEquals(7, lockManager.withLock(200L, Mono.just(7), () -> new IllegalStateException("BUSY")).block());
    }

    @Test
    @DisplayName("작업이 에러로 끝나도 락은 해제된다")
    void releasesOnError() {
        assertThrows(IllegalArgumentException.class,
                () -> lockManager.withLock(300L, Mono.error(new IllegalArgumentException("task fail")),
                        () -> new IllegalStateException("BUSY")).block());

        assertEquals(1, lockManager.withLock(300L, Mono.just(1), () -> new IllegalStateException("BUSY")).block());
    }

    @Test
    @DisplayName("구독 취소로 중단돼도 락은 해제된다")
    void releasesOnCancellation() throws Exception {
        CountDownLatch acquired = new CountDownLatch(1);

        Disposable holding = lockManager.withLock(400L,
                        Mono.<Integer>fromRunnable(acquired::countDown).then(Mono.never()),
                        () -> new IllegalStateException("BUSY"))
                .subscribe();
        assertTrue(acquired.await(2, TimeUnit.SECONDS));

        holding.dispose();

        assertEquals(1, lockManager.withLock(400L, Mono.just(1), () -> new IllegalStateException("BUSY")).block());
    }

    @Test
    @DisplayName("다른 방의 락은 서로 간섭하지 않는다")
    void roomsAreIndependent() throws Exception {
        CountDownLatch acquired = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        lockManager.withLock(500L, Mono.fromCallable(() -> {
                    acquired.countDown();
                    release.await(10, TimeUnit.SECONDS);
                    return 1;
                }), () -> new IllegalStateException("holder fail"))
                .subscribe();
        assertTrue(acquired.await(2, TimeUnit.SECONDS));

        assertEquals(List.of(1), lockManager.withLock(501L, Mono.just(List.of(1)),
                () -> new IllegalStateException("BUSY")).block());

        release.countDown();
    }
}
