package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class RoomService {
    private final GameStateRepository gameStateRepository;
    public Mono<Void> joinRoom(long userId, long roomId) {
        return gameStateRepository.findById(roomId)
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.NOT_EXISTED_ROOM)))
                .flatMap(gameState -> {
                    if (isRoomFull(gameState))
                        return Mono.error(new BusinessException(ErrorCode.FULL_ROOM));
                    if (isUserInRoom(gameState, userId))
                        return Mono.error(new BusinessException(ErrorCode.ALREADY_IN_ROOM));
                    return updateGameState(gameState, userId)
                            .then();
                });
    }

    private boolean isRoomFull(GameState gameState) {
        return gameState.getPlayer1Id() != null && gameState.getPlayer2Id() != null;
    }

    private boolean isUserInRoom(GameState gameState, Long userId) {
        return userId.equals(gameState.getPlayer1Id()) || userId.equals(gameState.getPlayer2Id());
    }

    private Mono<Void> updateGameState(GameState gameState, long userId) {
        if (gameState.getPlayer1Id() == null) {
            gameState.setPlayer1Id(userId);
        } else if (gameState.getPlayer2Id() == null) {
            gameState.setPlayer2Id(userId);
        }
        return gameStateRepository.save(gameState).then();
    }

    public Mono<Long> createRoom(Long roomId) {
        GameState gameState = new GameState();
        gameState.setRoomId(roomId);
        return gameStateRepository.create(gameState);
    }

    public Mono<Void> ready(Long userId, Long roomId) {
        return gameStateRepository.findById(roomId)
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.NOT_EXISTED_ROOM)))
                .flatMap(gameState -> {
                    if (gameState.getPlayer1Id().equals(userId)) {
                        gameState.setPlayer1Ready(true);
                    } else if (gameState.getPlayer2Id().equals(userId)) {
                        gameState.setPlayer2Ready(true);
                    } else {
                        return Mono.error(new BusinessException(ErrorCode.NOT_IN_ROOM));
                    }
                    return updateGameState(gameState, userId)
                            .then();
                });
    }

}
