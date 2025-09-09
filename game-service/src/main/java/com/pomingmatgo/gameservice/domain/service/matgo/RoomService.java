package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import org.springframework.stereotype.Service;

@Service
public class RoomService {
    GameStateRepository gameStateRepository;
    public long createRoom() {
        GameState gameState = new GameState();
        return gameStateRepository.save(gameState).getRoomId();
    }
}
