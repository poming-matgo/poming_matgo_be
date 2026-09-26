package com.pomingmatgo.gameservice.global.lock;

import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryRoomExecutionGateTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final InMemoryRoomExecutionGate gate = new InMemoryRoomExecutionGate();

    @Test
    void cleanupIsLazyWaitsForActionAndDoesNotBlockAnotherRoom() {
        var active = gate.acquire(1);
        AtomicInteger deletions = new AtomicInteger();
        Mono<Void> cleanup = gate.withCleanup(1, () -> Mono.fromRunnable(deletions::incrementAndGet));
        assertEquals(0, deletions.get());
        StepVerifier.create(cleanup)
                .then(() -> {
                    assertEquals(0, deletions.get());
                    assertBlocked();
                    var other = gate.acquire(2);
                    gate.release(other);
                    gate.discardIdle(2);
                    gate.release(active);
                })
                .expectComplete().verify(TIMEOUT);
        assertEquals(1, deletions.get());
        assertEmpty();
    }

    @Test
    void cancellingCleanupWaitDoesNotReleaseActiveAction() {
        var active = gate.acquire(1);
        AtomicInteger deletions = new AtomicInteger();
        StepVerifier.create(gate.withCleanup(1, () -> Mono.fromRunnable(deletions::incrementAndGet)))
                .then(this::assertBlocked)
                .thenCancel().verify(TIMEOUT);
        assertEquals(0, deletions.get());
        gate.discardIdle(1);
        assertThrows(WebSocketBusinessException.class, () -> gate.acquire(1));
        gate.release(active);
        StepVerifier.create(gate.withCleanup(1, Mono::empty)).verifyComplete();
        assertEmpty();
    }

    @Test
    void overlappingCleanupsKeepCreationBlockedUntilBothFinish() {
        Sinks.Empty<Void> first = Sinks.empty();
        Sinks.Empty<Void> second = Sinks.empty();
        var observer = gate.withCleanup(1, () -> first.asMono()).subscribe();
        try {
            StepVerifier.create(gate.withCleanup(1, () -> second.asMono()))
                    .then(() -> {
                        first.tryEmitEmpty();
                        assertBlocked();
                        gate.discardIdle(1);
                        assertBlocked();
                        second.tryEmitEmpty();
                    })
                    .expectComplete().verify(TIMEOUT);
            assertEmpty();
            assertEquals("created", gate.create(1, () -> "created"));
        } finally {
            observer.dispose();
        }
    }

    @Test
    void synchronousCleanupFailureReleasesBarrierAndPreservesError() {
        var failure = new IllegalStateException("cleanup failed");
        StepVerifier.create(gate.withCleanup(1, () -> { throw failure; }))
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        assertEmpty();
    }

    @Test
    void cancellationDuringDeletionReleasesBarrierAndCancelsDeletion() {
        Sinks.Empty<Void> deletion = Sinks.empty();
        StepVerifier.create(gate.withCleanup(1, () -> deletion.asMono()))
                .then(this::assertBlocked)
                .thenCancel().verify(TIMEOUT);
        assertEquals(0, deletion.currentSubscriberCount());
        assertEmpty();
    }

    private void assertBlocked() {
        assertThrows(WebSocketBusinessException.class, () -> gate.acquire(1));
        assertThrows(BusinessException.class, () -> gate.create(1, () -> "created"));
    }

    private void assertEmpty() {
        assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(gate, "rooms")).isEmpty());
    }
}
