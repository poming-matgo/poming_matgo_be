package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLockReactive;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_NOTHING;

@Component
@RequiredArgsConstructor
@Slf4j
public class WsRoomHandler {
    private final MessageSender messageSender;
    private final RoomService roomService;
    private final PreGameService preGameService;
    private final RedissonReactiveClient redissonReactiveClient;

    public Mono<Void> handleRoomEvent(RequestEvent<?> event, GameState gameState, Player player) {
        SubCategory eventType = SubCategory.from(event.getEventType().getSubType());

        return switch (eventType) {
            case READY -> handleReadyEvent(gameState, player);
            case UNREADY -> handleUnreadyEvent(gameState, player);
            default -> Mono.error(new IllegalArgumentException("Invalid GAME event type"));

        };
    }

    private Mono<Void> handleReadyEvent(GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        RLockReactive lock = redissonReactiveClient.getLock("READY_LOCK:ROOM:" + roomId);

        return Mono.usingWhen(
                lock.tryLock(5, 2, TimeUnit.SECONDS)
                        .flatMap(acquired -> acquired
                                ? Mono.just(lock)
                                : Mono.error(new WebSocketBusinessException(WebSocketErrorCode.TOO_MANY_REQUESTS))),
                acquiredLock -> roomService.readyFresh(roomId, player, true)
                        .flatMap(freshState ->
                                messageSender.sendMessageToAllUser(
                                                roomId,
                                                WebSocketResDto.of(player, "READY", "Ready 했습니다.")
                                        )
                                        .then(checkAndProceedIfAllReady(freshState))
                        ),
                this::releaseLock,
                (acquiredLock, err) -> releaseLock(acquiredLock),
                this::releaseLock
        );
    }

    private Mono<Void> handleUnreadyEvent(GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        RLockReactive lock = redissonReactiveClient.getLock("READY_LOCK:ROOM:" + roomId);

        return Mono.usingWhen(
                lock.tryLock(5, 2, TimeUnit.SECONDS)
                        .flatMap(acquired -> acquired
                                ? Mono.just(lock)
                                : Mono.error(new WebSocketBusinessException(WebSocketErrorCode.TOO_MANY_REQUESTS))),
                acquiredLock -> roomService.readyFresh(roomId, player, false)
                        .then(messageSender.sendMessageToAllUser(
                                roomId,
                                WebSocketResDto.of(player, "UNREADY", "Ready 취소 했습니다.")
                        )),
                this::releaseLock,
                (acquiredLock, err) -> releaseLock(acquiredLock),
                this::releaseLock
        );
    }

    private Mono<Void> releaseLock(RLockReactive lock) {
        return lock.forceUnlock()
                .then()
                .onErrorResume(e -> {
                    return Mono.empty();
                });
    }

    private Mono<Void> checkAndProceedIfAllReady(GameState updatedGameState) {
        return Mono.just(updatedGameState)
                .filter(roomService::checkAllPlayersReady)
                .flatMap(roomService::startGame)
                .flatMap(state -> preGameService.pickFiveCardsAndSave(state.getRoomId())
                        .then(handleAllReadyEvent(state.getRoomId()))
                );
    }

    private Mono<Void> handleAllReadyEvent(long roomId) {
        WebSocketResDto<Void> startDto = new WebSocketResDto<>(
                PLAYER_NOTHING,
                "START",
                "게임이 시작됐습니다."
        );
        return messageSender.sendMessageToAllUser(roomId, startDto);
    }
}
