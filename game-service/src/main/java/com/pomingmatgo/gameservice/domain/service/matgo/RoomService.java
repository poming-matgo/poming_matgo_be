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
                .filter(gameState -> !isRoomFull(gameState))
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.FULL_ROOM)))
                .filter(gameState -> !isUserInRoom(gameState, userId))
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.ALREADY_IN_ROOM)))
                .flatMap(gameState -> saveWithUserId(gameState, userId))
                .then();
    }

    public Mono<Void> leaveRoom(long userId, long roomId) {

        return gameStateRepository.findById(roomId)
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.NOT_EXISTED_ROOM)))
                .flatMap(gameState -> {
                    GameState.GameStateBuilder builder = gameState.toBuilder();
                    if(gameState.getPlayer1Id() == userId) {
                        builder.player1Id(null);
                        builder.player1Ready(false);
                    }
                    else if(gameState.getPlayer2Id() == userId) {
                        builder.player2Id(null);
                        builder.player2Ready(false);
                    }
                    return saveWithUserId(builder.build(), userId)
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
        GameState.GameStateBuilder builder = gameState.toBuilder();
        if (gameState.getPlayer1Id() == null) {
            builder.player1Id(userId);
        } else if (gameState.getPlayer2Id() == null) {
            builder.player2Id(userId);
        }
        return gameStateRepository.save(builder.build()).then();
    }

    public Mono<Long> createRoom(Long roomId) {
        GameState gameState = new GameState(roomId);

        return sessionManager.addRoom(roomId)
                .then(gameStateRepository.create(gameState))
                .thenReturn(roomId)
                .onErrorResume(ex -> {
                    if (ex instanceof WebSocketBusinessException) {
                        BusinessException businessEx = (BusinessException) ex;
                        if (businessEx.getErrorCode() == ErrorCode.ALREADY_EXISTED_ROOM) {
                            return Mono.error(ex);
                        }
                    }

                    return sessionManager.removeRoom(roomId)
                            .then(Mono.error(ex));
                });
    }

    public Mono<GameState> ready(GameState gameState, int playerNum, boolean flag) {
        if (gameState == null) {
            return Mono.error(new WebSocketBusinessException(WebSocketErrorCode.NOT_EXISTED_ROOM));
        }

        GameState.GameStateBuilder builder = gameState.toBuilder();

        if (playerNum == 1) {
            builder.player1Ready(flag);
        } else {
            builder.player2Ready(flag);
        }

        return gameStateRepository.save(builder.build())
                .thenReturn(gameState);
    }


    public boolean checkAllPlayersReady(GameState gs) {
        return gs.isPlayer1Ready() && gs.isPlayer2Ready();
    }

}
