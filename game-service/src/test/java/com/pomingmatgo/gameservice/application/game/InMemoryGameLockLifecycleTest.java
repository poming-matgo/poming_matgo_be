package com.pomingmatgo.gameservice.application.game;

import com.pomingmatgo.gameservice.infrastructure.lock.GameLock;
import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryGameLockAspect;

import com.pomingmatgo.gameservice.infrastructure.lock.InMemoryRoomExecutionGate;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.function.Supplier;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN;
import static org.junit.jupiter.api.Assertions.*;

class InMemoryGameLockLifecycleTest {

    private LockedAction action;
    private InMemoryGameActionExecutor executor;
    private InMemoryRoomExecutionGate gate;

    @BeforeEach
    void setUp() {
        AspectJProxyFactory factory = new AspectJProxyFactory(new LockedAction());
        gate = new InMemoryRoomExecutionGate();
        executor = new InMemoryGameActionExecutor(gate);
        factory.addAspect(new InMemoryGameLockAspect(executor));
        action = factory.getProxy();
    }

    @AfterEach
    void shutdown() {
        executor.shutdown();
        assertEquals(0, ((Set<?>) ReflectionTestUtils.getField(executor, "executions")).size());
    }

    @Test
    void cancellationAtSubscriptionDoesNotStartOrRetainExecution() {
        AtomicInteger entered = new AtomicInteger();
        StepVerifier.create(action.run(1L, () -> Mono.just("entered " + entered.incrementAndGet())))
                .thenCancel().verify(Duration.ofSeconds(3));
        assertEquals(0, entered.get());
        assertEquals(0, ((Set<?>) ReflectionTestUtils.getField(executor, "executions")).size());
        assertExclusiveAfterRelease();
    }

    @Test
    void acceptedExecutionKeepsLockAndCleanupWaitingAfterCallerCancellation() {
        Sinks.One<String> finish = Sinks.one();
        AtomicInteger cleaned = new AtomicInteger();
        StepVerifier.create(action.run(1L, () -> GameActionAcceptance.beforeMutation(finish::asMono)))
                .then(() -> assertEquals(1, finish.currentSubscriberCount()))
                .thenCancel().verify(Duration.ofSeconds(3));
        assertEquals(1, finish.currentSubscriberCount());
        assertEquals(1, ((Set<?>) ReflectionTestUtils.getField(executor, "executions")).size());
        StepVerifier.create(executor.withCleanup(1, () -> Mono.fromRunnable(cleaned::incrementAndGet)))
                .then(() -> {
                    assertEquals(0, cleaned.get());
                    busy(action.run(1L, () -> Mono.just("contender")));
                    completes(action.run(2L, () -> Mono.just("other")), "other");
                    finish.tryEmitValue("done");
                }).verifyComplete();
        assertEquals(1, cleaned.get());
        assertEquals(0, ((Set<?>) ReflectionTestUtils.getField(executor, "executions")).size());
        assertExclusiveAfterRelease();
    }

    @Test
    void cleanupStartingBeforeAcceptanceRejectsMutation() {
        Sinks.Empty<Void> validation = Sinks.empty();
        AtomicInteger mutations = new AtomicInteger();
        Sinks.Empty<Void> cleaned = Sinks.empty();
        StepVerifier.create(action.run(1L, () -> validation.asMono().then(
                        GameActionAcceptance.beforeMutation(() -> Mono.fromSupplier(() -> "changed " + mutations.incrementAndGet())))))
                .then(() -> {
                    executor.withCleanup(1, Mono::empty).subscribe(ignored -> {}, cleaned::tryEmitError, cleaned::tryEmitEmpty);
                    validation.tryEmitEmpty();
                })
                .expectErrorSatisfies(error -> assertEquals(TRY_AGAIN,
                        assertInstanceOf(WebSocketBusinessException.class, error).getWebsocketErrorCode()))
                .verify(Duration.ofSeconds(3));
        cleaned.asMono().block(Duration.ofSeconds(3));
        assertEquals(0, mutations.get());
        assertExclusiveAfterRelease();
    }

    @Test
    void shutdownCancelsAcceptedExecutionReleasesLockAndRejectsNewSubscriptions() {
        Sinks.One<String> finish = Sinks.one();
        StepVerifier.create(action.run(1L, () -> GameActionAcceptance.beforeMutation(finish::asMono)))
                .then(executor::shutdown)
                .expectError(java.util.concurrent.CancellationException.class).verify(Duration.ofSeconds(3));
        assertEquals(0, finish.currentSubscriberCount());
        gate.release(gate.acquire(1));
        StepVerifier.create(action.run(1L, () -> Mono.just("late")))
                .expectErrorMessage("Game action execution stopped").verify(Duration.ofSeconds(3));
    }

    @Test
    void callerContextIsAvailableInsideOwnedExecution() {
        completes(action.run(1L, () -> GameActionAcceptance.beforeMutation(() ->
                Mono.deferContextual(context -> Mono.just(context.get("request")))))
                .contextWrite(context -> context.put("request", "correlation")), "correlation");
    }

    @Test
    void unSubscribedMonoDoesNotAcquireLock() {
        Mono<String> unused = action.run(1L, () -> Mono.just("unused"));

        completes(action.run(1L, () -> Mono.just("next")), "next");
        completes(unused, "unused");
    }

    @Test
    void contentionIsDecidedAtSubscription() {
        Mono<String> prepared = action.run(1L, () -> Mono.just("prepared"));

        StepVerifier.create(action.run(1L, Mono::never))
                .then(() -> busy(prepared))
                .then(() -> completes(action.run(2L, () -> Mono.just("other")), "other"))
                .thenCancel()
                .verify(Duration.ofSeconds(3));

        completes(prepared, "prepared");
    }

    @Test
    void concurrentSubscriptionsToSameMonoCannotEnterTogether() {
        AtomicInteger entered = new AtomicInteger();
        Mono<String> pending = action.run(1L, () -> {
            entered.incrementAndGet();
            return Mono.never();
        });

        StepVerifier.create(pending)
                .then(() -> busy(pending))
                .then(() -> assertEquals(1, entered.get()))
                .thenCancel()
                .verify(Duration.ofSeconds(3));

        completes(action.run(1L, () -> Mono.just("after cancel")), "after cancel");
    }

    @Test
    void resubscriptionAfterCompletionReacquiresExactlyOnePermit() {
        AtomicInteger entered = new AtomicInteger();
        Mono<String> repeated = action.run(1L, () -> entered.incrementAndGet() == 1
                ? Mono.just("first") : Mono.never());

        completes(repeated, "first");
        StepVerifier.create(repeated)
                .then(() -> busy(action.run(1L, () -> Mono.just("contender"))))
                .thenCancel()
                .verify(Duration.ofSeconds(3));

        assertExclusiveAfterRelease();
    }

    @Test
    void publisherErrorReleasesLock() {
        StepVerifier.create(action.run(1L, () -> Mono.error(new IllegalArgumentException("failure"))))
                .expectErrorMessage("failure")
                .verify(Duration.ofSeconds(3));

        assertExclusiveAfterRelease();
    }

    @Test
    void synchronousMethodErrorReleasesLock() {
        StepVerifier.create(action.run(1L, () -> {
                    throw new IllegalArgumentException("assembly failure");
                }))
                .expectErrorMessage("assembly failure")
                .verify(Duration.ofSeconds(3));

        assertExclusiveAfterRelease();
    }

    @Test
    void retryReacquiresWithoutIncreasingPermits() {
        AtomicInteger attempts = new AtomicInteger();
        Mono<String> retried = action.run(1L, () -> attempts.incrementAndGet() == 1
                ? Mono.error(new IllegalArgumentException("first failure"))
                : Mono.just("retried"));

        completes(retried.retry(1), "retried");
        assertEquals(2, attempts.get());
        assertExclusiveAfterRelease();
    }

    private void assertExclusiveAfterRelease() {
        StepVerifier.create(action.run(1L, Mono::never))
                .then(() -> busy(action.run(1L, () -> Mono.just("contender"))))
                .thenCancel()
                .verify(Duration.ofSeconds(3));
        completes(action.run(1L, () -> Mono.just("released")), "released");
    }

    private void busy(Mono<String> request) {
        StepVerifier.create(request)
                .expectErrorSatisfies(error -> {
                    assertEquals(WebSocketBusinessException.class, error.getClass());
                    assertEquals(TRY_AGAIN, ((WebSocketBusinessException) error).getWebsocketErrorCode());
                })
                .verify(Duration.ofSeconds(3));
    }

    private void completes(Mono<String> request, String expected) {
        StepVerifier.create(request)
                .expectNext(expected)
                .expectComplete()
                .verify(Duration.ofSeconds(3));
    }

    public static class LockedAction {
        @GameLock
        public Mono<String> run(long roomId, Supplier<Mono<String>> operation) {
            return operation.get();
        }
    }
}
