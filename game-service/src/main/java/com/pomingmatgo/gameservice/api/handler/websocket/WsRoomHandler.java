package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class WsRoomHandler {
    private final MessageSender messageSender;
    private final RoomService roomService;
    private final PreGameService preGameService;
    public Mono<Void> handleRoomEvent(RequestEvent<?> event, GameState gameState, int playerNum) {
        String eventType = event.getEventType().getSubType();
        long roomId = gameState.getRoomId();
        if ("CONNECT".equals(eventType)) {
            return messageSender.sendMessageToAllUser(roomId, new WebSocketResDto<>(
                    playerNum,
                    "CONNECT",
                    "접속했습니다."
            ));
        }
        else if ("READY".equals(eventType)) {
            return roomService.ready(gameState, playerNum, true)
                    .flatMap(updatedGameState ->
                            messageSender.sendMessageToAllUser(
                                            roomId,
                                            new WebSocketResDto<>(playerNum, "READY", "Ready 했습니다.")
                                    )
                                    .then(Mono.defer(() -> {
                                        boolean allReady = roomService.checkAllPlayersReady(updatedGameState);
                                        return allReady
                                                ? handleAllReadyEvent(roomId)
                                                .then(Mono.defer(() -> preGameService.pickFiveCardsAndSave(updatedGameState.getRoomId())))
                                                : Mono.empty();
                                    }))
                    );

        }
        else if ("UNREADY".equals(eventType)) {
            return roomService.ready(gameState, playerNum, true)
                    .then(
                            messageSender.sendMessageToAllUser(
                                    roomId,
                                    new WebSocketResDto<>(
                                            playerNum,
                                            "UNREADY",
                                            "Ready 취소 했습니다."
                                    )
                            )
                    )
                    .then(Mono.empty());
        }
        return Mono.empty();
    }

    private Mono<Void> handleAllReadyEvent(long roomId) {
        // 게임이 시작되었다는 메시지를 모든 사용자에게 전송
        WebSocketResDto<Void> startDto = new WebSocketResDto<>(
                0,
                "START",
                "게임이 시작됐습니다."
        );
        return messageSender.sendMessageToAllUser(roomId, startDto);
    }
}
