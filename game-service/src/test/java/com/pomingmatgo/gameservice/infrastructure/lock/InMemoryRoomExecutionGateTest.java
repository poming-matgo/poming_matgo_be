package com.pomingmatgo.gameservice.infrastructure.lock;

import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import reactor.util.context.ContextView;

import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryRoomExecutionGateTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final InMemoryRoomExecutionGate gate = new InMemoryRoomExecutionGate();

    @Test
    void failedActionRemainsBlockedWhenCleanupFailsOrIsCancelled() {
        var active = gate.acquire(1);
        gate.fail(active);
        gate.release(active);
        assertBlocked();
        StepVerifier.create(gate.withCleanup(1, () -> Mono.error(new IllegalStateException("cleanup failed"))))
                .expectErrorMessage("cleanup failed").verify(TIMEOUT);
        assertBlocked();
        StepVerifier.create(gate.withCleanup(1, Mono::never))
                .then(this::assertBlocked).thenCancel().verify(TIMEOUT);
        assertBlocked();
        StepVerifier.create(gate.withCleanup(1, Mono::empty)).verifyComplete();
        assertEmpty();
        assertEquals("created", gate.create(1, () -> "created"));
    }

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
                    StepVerifier.create(gate.withCleanup(2, Mono::empty)).verifyComplete();
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
        assertThrows(WebSocketBusinessException.class, () -> gate.acquire(1));
        gate.release(active);
        StepVerifier.create(gate.withCleanup(1, Mono::empty)).verifyComplete();
        assertEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void duplicateCleanupIsRejectedWithoutChangingOwnersResult(boolean failed) {
        Sinks.Empty<Void> first = Sinks.empty();
        AtomicInteger duplicateDeletions = new AtomicInteger();
        var failure = new IllegalStateException("owner cleanup failed");
        var verification = StepVerifier.create(gate.withCleanup(1, first::asMono))
                .then(() -> {
                    StepVerifier.create(gate.withCleanup(1,
                                    () -> Mono.fromRunnable(duplicateDeletions::incrementAndGet)))
                            .expectErrorMessage("Room cleanup already active: 1").verify(TIMEOUT);
                    assertEquals(0, duplicateDeletions.get());
                    assertEquals(1, first.currentSubscriberCount());
                    assertBlocked();
                    if (failed) first.tryEmitError(failure);
                    else first.tryEmitEmpty();
                });
        if (failed) {
            verification.expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
            assertBlocked();
            StepVerifier.create(gate.withCleanup(1, Mono::empty)).verifyComplete();
        } else {
            verification.expectComplete().verify(TIMEOUT);
        }
        assertEmpty();
        assertEquals("created", gate.create(1, () -> "created"));
    }

    @Test
    void synchronousCleanupFailurePreservesErrorAndBlocksUntilSuccessfulCleanup() {
        var failure = new IllegalStateException("cleanup failed");
        StepVerifier.create(gate.withCleanup(1, () -> { throw failure; }))
                .expectErrorSatisfies(error -> assertSame(failure, error)).verify(TIMEOUT);
        assertBlocked();
        StepVerifier.create(gate.withCleanup(1, Mono::empty)).verifyComplete();
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

    @Test
    void restartPermissionCannotCreateInAnotherRoomOrSurviveRelease() {
        var active = gate.acquire(1);
        AtomicReference<ContextView> captured = new AtomicReference<>();
        StepVerifier.create(gate.inRestart(active, () -> Mono.deferContextual(context -> {
                    captured.set(context);
                    assertThrows(BusinessException.class, () -> gate.create(2, context, () -> "wrong room"));
                    return Mono.just(gate.create(1, context, () -> "recreated"));
                })))
                .expectNext("recreated").verifyComplete();
        gate.release(active);
        assertThrows(BusinessException.class, () -> gate.create(1, captured.get(), () -> "late"));
        var nextAction = gate.acquire(1);
        assertThrows(BusinessException.class, () -> gate.create(1, captured.get(), () -> "stale owner"));
        gate.release(nextAction);
    }

    private void assertEmpty() {
        assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(gate, "rooms")).isEmpty());
    }
}
