package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

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

    public Mono<Void> ready(Mono<GameState> gameState, int playerNum) {
        return gameState
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.NOT_EXISTED_ROOM)))
                .flatMap(gs -> {
                    if (playerNum == 1)
                        gs.setPlayer1Ready(true);
                    else
                        gs.setPlayer2Ready(true);

                    Collection<WebSocketSession> allUser = sessionManager.getAllUser(gs.getRoomId());

                    return Flux.fromIterable(allUser)
                            .flatMap(session -> {
                                WebSocketMessage message = session.textMessage("test");
                                return session.send(Mono.just(message));
                            })
                            .then(gameStateRepository.save(gs))
                            .then();
                });
    }

}
