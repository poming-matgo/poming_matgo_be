package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLockReactive;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.pomingmatgo.gameservice.domain.GamePhase.DETERMINING_STARTING_PLAYER;


@Service
@RequiredArgsConstructor
public class RoomService {
    private final GameStateRepository gameStateRepository;
    private final SessionManager sessionManager;
    private final RedissonReactiveClient redissonReactiveClient;

    public Mono<Void> joinRoom(long userId, long roomId) {
        RLockReactive lock = redissonReactiveClient.getLock("READY_LOCK:ROOM:" + roomId);
        long executionId = UUID.randomUUID().getMostSignificantBits();

        return Mono.usingWhen(
                lock.tryLock(5, 2, TimeUnit.SECONDS, executionId)
                        .flatMap(acquired -> acquired
                                ? Mono.just(lock)
                                : Mono.error(new BusinessException(ErrorCode.SYSTEM_ERROR))),
                acquiredLock -> gameStateRepository.findById(roomId)
                        .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.NOT_EXISTED_ROOM)))
                        .filter(gameState -> !isRoomFull(gameState))
                        .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.FULL_ROOM)))
                        .filter(gameState -> !isUserInRoom(gameState, userId))
                        .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.ALREADY_IN_ROOM)))
                        .flatMap(gameState -> saveWithUserId(gameState, userId))
                        .then(),
                acquiredLock -> releaseLock(acquiredLock, executionId),
                (acquiredLock, err) -> releaseLock(acquiredLock, executionId),
                acquiredLock -> releaseLock(acquiredLock, executionId)
        );
    }

    private Mono<Void> releaseLock(RLockReactive lock, long executionId) {
        return lock.unlock(executionId)
                .then()
                .onErrorResume(e -> Mono.empty());
    }

    public Mono<Void> leaveRoom(long userId, long roomId) {
        return gameStateRepository.findById(roomId)
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.NOT_EXISTED_ROOM)))
                .filter(gameState -> isUserInRoom(gameState, userId))
                .flatMap(gameState -> {
                    Player player = gameState.getPlayerType(userId);

                    GameState newState = gameState.updatePlayerState(player, new PlayerState());
                    return gameStateRepository.save(newState);
                })
                .then();
    }

    private boolean isUserInRoom(GameState gameState, long userId) {
        return gameState.hasUser(userId);
    }

    public Mono<Void> deleteRoom(long roomId) {
        return gameStateRepository.delete(roomId)
                .then();
    }

    private boolean isRoomFull(GameState gameState) {
        return gameState.isRoomFull();
    }

    public Mono<GameState> getGameState(Long roomId) {
        return gameStateRepository.findById(roomId);
    }

    private Mono<Void> saveWithUserId(GameState gameState, long userId) {
        if (!gameState.canJoin()) {
            return Mono.error(new WebSocketBusinessException(WebSocketErrorCode.FULL_ROOM));
        }
        GameState newState = gameState.join(userId);
        return gameStateRepository.save(newState).then();
    }

    public Mono<Long> createRoom(Long roomId) {
        GameState gameState = GameState.createEmptyRoom(roomId);

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

    public Mono<GameState> ready(GameState gameState, Player player, boolean isReady) {
        if (gameState == null) {
            return Mono.error(new WebSocketBusinessException(WebSocketErrorCode.NOT_EXISTED_ROOM));
        }

        GameState newState = gameState.withPlayerReady(player, isReady);
        return gameStateRepository.save(newState)
                .thenReturn(newState);
    }

    public Mono<GameState> readyFresh(long roomId, Player player, boolean isReady) {
        return gameStateRepository.findById(roomId)
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(WebSocketErrorCode.NOT_EXISTED_ROOM)))
                .map(state -> state.withPlayerReady(player, isReady))
                .flatMap(state -> gameStateRepository.save(state).thenReturn(state));
    }


    public boolean checkAllPlayersReady(GameState gs) {
        return gs.allPlayersReady();
    }

    public Mono<GameState> startGame(GameState gameState) {
        if (gameState == null) {
            return Mono.error(new WebSocketBusinessException(WebSocketErrorCode.NOT_EXISTED_ROOM));
        }

        GameState.GameStateBuilder builder = gameState.toBuilder();
        builder.gameStarted(true)
                .phase(DETERMINING_STARTING_PLAYER);

        GameState newState = builder.build();
        return gameStateRepository.save(newState)
                .thenReturn(newState);
    }
}
