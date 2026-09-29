package com.pomingmatgo.gameservice.api.handler;

import com.pomingmatgo.gameservice.application.game.InMemoryGameActionExecutor;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.application.room.RoomService;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@DisplayName("HTTP Join의 게임 락 경합 응답")
class RoomJoinHttpTest {
    private static final long ROOM_ID = 940_052L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    @Autowired ApplicationContext context;
    @Autowired RoomService rooms;
    @Autowired RoomCleanupService cleanup;
    @Autowired GameStateRepository states;
    @Autowired InMemoryGameActionExecutor executor;

    @AfterEach
    void tearDown() {
        cleanup.cleanupRoom(ROOM_ID).block(TIMEOUT);
    }

    @Test
    @DisplayName("경합은 HTTP 409 TRY_AGAIN이고 해제 뒤 명시적으로 재시도하면 성공한다")
    void contentionReturnsConflictAndCanRetry() {
        rooms.createRoom(ROOM_ID).block(TIMEOUT);
        WebTestClient client = WebTestClient.bindToApplicationContext(context).build();
        var holding = executor.execute(ROOM_ID, Mono::never).subscribe();
        try {
            join(client, 101L).expectStatus().isEqualTo(409)
                    .expectBody().jsonPath("$.errorCode").isEqualTo("TRY_AGAIN")
                    .jsonPath("$.httpStatus").isEqualTo(409);
            assertFalse(states.findById(ROOM_ID).block(TIMEOUT).hasUser(101L));
        } finally {
            holding.dispose();
        }
        join(client, 101L).expectStatus().isOk();
        assertTrue(states.findById(ROOM_ID).block(TIMEOUT).hasUser(101L));
        join(client, 101L).expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.errorCode").isEqualTo("ALREADY_IN_ROOM");
        join(client, 202L).expectStatus().isOk();
        join(client, 303L).expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.errorCode").isEqualTo("FULL_ROOM");
    }

    private WebTestClient.ResponseSpec join(WebTestClient client, long userId) {
        return client.post().uri("/room/join").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("roomId", ROOM_ID, "userId", userId)).exchange();
    }
}
