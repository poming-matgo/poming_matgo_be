package com.pomingmatgo.gameservice.infrastructure.session;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ActionNotificationOrderTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    @Test
    void cancelledMiddleReservationDoesNotLetSuccessorOvertakeHead() {
        ActionNotificationOrder order = new ActionNotificationOrder();
        var first = order.reserve();
        var cancelled = order.reserve();
        var last = order.reserve();
        List<Boolean> permission = new ArrayList<>();
        last.ready().subscribe(permission::add);
        cancelled.dispose();
        assertEquals(List.of(), permission);
        first.dispose();
        assertEquals(List.of(true), permission);
        last.dispose();
        var next = order.reserve();
        StepVerifier.create(next.ready()).expectNext(true).expectComplete().verify(TIMEOUT);
        next.dispose();
    }

    @Test
    void cleanupRejectsWaitersAndLateReservations() {
        ActionNotificationOrder order = new ActionNotificationOrder();
        var first = order.reserve();
        var second = order.reserve();
        StepVerifier.create(second.ready())
                .then(order::close).expectNext(false).expectComplete().verify(TIMEOUT);
        first.dispose();
        StepVerifier.create(order.reserve().ready()).expectNext(false).expectComplete().verify(TIMEOUT);
    }

    @Test
    void subscriptionOrderDoesNotReplaceReservationOrder() {
        ActionNotificationOrder order = new ActionNotificationOrder();
        var first = order.reserve();
        var second = order.reserve();
        StepVerifier.create(second.ready())
                .then(() -> StepVerifier.create(first.ready()).expectNext(true).expectComplete().verify(TIMEOUT))
                .then(first::dispose).expectNext(true).expectComplete().verify(TIMEOUT);
        second.dispose();
    }
}
