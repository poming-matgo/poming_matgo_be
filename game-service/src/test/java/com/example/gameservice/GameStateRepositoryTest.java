package com.example.gameservice;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.*;

class GameStateServiceTest {

    @Test
    void testFindById() {
        // Mock Redis operations
        /*ReactiveRedisOperations<String, Object> redisOps = mock(ReactiveRedisOperations.class);
        ReactiveValueOperations<String, Object> valueOps = mock(ReactiveValueOperations.class);

        String redisKey = "gameState:2";
        GameState expectedGameState = new GameState(); // Assume this is a valid GameState object
        expectedGameState.setRoomId(2L);

        // Mock behavior
        when(redisOps.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(redisKey)).thenReturn(Mono.just(expectedGameState));

        // Call the method
        GameStateRepository gameStateRepository = new GameStateRepository(redisOps);
        Mono<GameState> result = gameStateRepository.findById(2L);

        // Verify the result
        StepVerifier.create(result)
                .expectNext(expectedGameState)
                .verifyComplete();

        // Verify interactions
        verify(valueOps, times(1)).get(redisKey);
    }

    @Test
    void testFindById_empty() {
        // Mock Redis operations
        ReactiveRedisOperations<String, Object> redisOps = mock(ReactiveRedisOperations.class);
        ReactiveValueOperations<String, Object> valueOps = mock(ReactiveValueOperations.class);

        String redisKey = "gameState:123";

        // Mock behavior for empty Mono
        when(redisOps.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(redisKey)).thenReturn(Mono.empty());

        // Call the method
        GameStateRepository gameStateRepository = new GameStateRepository(redisOps);
        Mono<GameState> result = gameStateRepository.findById(123L);

        // Verify the result
        StepVerifier.create(result)
                .verifyComplete(); // No items emitted

        // Verify interactions
        verify(valueOps, times(1)).get(redisKey);*/
    }
}