package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class RoomService {
    private final GameStateRepository gameStateRepository;
    public Mono<Long> joinRoom(long userId) {
        GameState gameState = new GameState();
        return gameStateRepository.save(gameState)
                .map(roomId -> {
                    return roomId;
                });
    }

}
