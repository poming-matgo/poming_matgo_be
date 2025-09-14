package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;


@Service
@RequiredArgsConstructor
public class RoomService {
    private final GameStateRepository gameStateRepository;
    private final SessionManager sessionManager;
    public Mono<Void> joinRoom(long userId, long roomId) {
        return gameStateRepository.findById(roomId)
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.NOT_EXISTED_ROOM)))
                .flatMap(gameState -> {
                    if (isRoomFull(gameState))
                        return Mono.error(new BusinessException(ErrorCode.FULL_ROOM));
                    if (isUserInRoom(gameState, userId))
                        return Mono.error(new BusinessException(ErrorCode.ALREADY_IN_ROOM));
                    return saveWithUserId(gameState, userId)
                            .then();
                });
    }

    public Mono<Void> leaveRoom(long userId, long roomId) {
        return gameStateRepository.findById(roomId)
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.NOT_EXISTED_ROOM)))
                .flatMap(gameState -> {
                    if(gameState.getPlayer1Id() == userId) {
                        gameState.setPlayer1Id(null);
                        gameState.setPlayer1Ready(false);
                    }
                    else if(gameState.getPlayer2Id() == userId) {
                        gameState.setPlayer2Id(null);
                        gameState.setPlayer2Ready(false);
                    }
                    return saveWithUserId(gameState, userId)
                            .then();
                });
    }

    public Mono<Void> deleteRoom(long roomId) {
        return gameStateRepository.delete(roomId)
                .then();
    }

    private boolean isRoomFull(GameState gameState) {
        return gameState.getPlayer1Id() != null && gameState.getPlayer2Id() != null;
    }

    private boolean isUserInRoom(GameState gameState, Long userId) {
        return userId.equals(gameState.getPlayer1Id()) || userId.equals(gameState.getPlayer2Id());
    }

    public Mono<GameState> getGameState(Long roomId) {
        return gameStateRepository.findById(roomId);
    }

    private Mono<Void> saveWithUserId(GameState gameState, long userId) {
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
        sessionManager.addRoom(roomId);
        return gameStateRepository.create(gameState);
    }

    public Mono<GameState> ready(Mono<GameState> gameState, int playerNum, boolean flag) {
        return gameState
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(WebSocketErrorCode.NOT_EXISTED_ROOM)))
                .flatMap(gs -> {
                    if (playerNum == 1)
                        gs.setPlayer1Ready(flag);
                    else
                        gs.setPlayer2Ready(flag);

                    return gameStateRepository.save(gs)
                            .thenReturn(gs);
                });
    }

    public Mono<Boolean> checkAllPlayersReady(Mono<GameState> gameState) {
        return gameState.map(gs ->
                gs.isPlayer1Ready() && gs.isPlayer2Ready()
        );
    }

}
