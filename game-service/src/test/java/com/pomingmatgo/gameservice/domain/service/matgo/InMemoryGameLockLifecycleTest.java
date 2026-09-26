package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.global.lock.InMemoryRoomExecutionGate;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN;
import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryGameLockLifecycleTest {

    private LockedAction action;

    @BeforeEach
    void setUp() {
        AspectJProxyFactory factory = new AspectJProxyFactory(new LockedAction());
        factory.addAspect(new InMemoryGameLockAspect(new InMemoryRoomExecutionGate()));
        action = factory.getProxy();
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