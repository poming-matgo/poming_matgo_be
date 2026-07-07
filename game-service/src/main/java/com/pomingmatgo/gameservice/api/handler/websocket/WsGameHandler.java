package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.request.websocket.GoStopReq;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.api.response.websocket.GameMessageSender;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.*;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.scheduler.AutoPlayScheduler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.domain.GamePhase.*;
import static com.pomingmatgo.gameservice.domain.TurnTiming.TURN_TIMEOUT_MILLIS;
import static com.pomingmatgo.gameservice.domain.TurnTiming.nextDeadlineNanos;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.NOT_YOUR_TURN;

@Component
@RequiredArgsConstructor
@Slf4j
public class WsGameHandler {

    private final GamePlayService gamePlayService;
    private final TurnFlowService turnFlowService;
    private final GameMessageSender gameMessageSender;
    private final AutoPlayScheduler autoPlayScheduler;

    public Mono<Void> handleGameEvent(RequestEvent<?> event, GameState gameState, Player player) {
        if (!player.equals(gameState.getCurrentPlayer())) {
            return Mono.error(new WebSocketBusinessException(NOT_YOUR_TURN));
        }

        SubCategory eventType = SubCategory.from(event.getEventType().getSubType());

        return switch (eventType) {
            case NORMAL_SUBMIT -> handleNormalSubmit(event.as(), gameState, player);
            case FLOOR_SELECT -> handleFloorSelect(event.as(), gameState, player);
            case GO_STOP_CHOICE -> handleGoStopChoice(event.as(), gameState, player);
            default -> Mono.error(new IllegalArgumentException("Invalid GAME event type"));
        };
    }

    private Mono<Void> handleNormalSubmit(RequestEvent<NormalSubmitReq> event, GameState gameState, Player player) {
        if (gameState.getPhase() != IN_PROGRESS) {
            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
        }

        long roomId = gameState.getRoomId();
        return turnFlowService.processNormalSubmit(
                roomId, gameState, player, event.getData().cardIndex(),
                () -> autoPlayScheduler.cancelAutoPlay(roomId), autoPlayScheduler);
    }

    private Mono<Void> handleFloorSelect(RequestEvent<NormalSubmitReq> event, GameState gameState, Player player) {
        if (gameState.getPhase() != AWAITING_FLOOR_CARD_CHOICE) {
            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
        }

        long roomId = gameState.getRoomId();
        return turnFlowService.processFloorSelection(
                roomId, gameState, player, event.getData().cardIndex(),
                () -> autoPlayScheduler.cancelAutoPlay(roomId), autoPlayScheduler);
    }

    private Mono<Void> handleGoStopChoice(RequestEvent<GoStopReq> event, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();

        return gamePlayService.executeGoStop(roomId, gameState, player, event, () -> autoPlayScheduler.cancelAutoPlay(roomId))
                .delayUntil(gs -> {
                    if (gs.isPlaying()) {
                        return gameMessageSender.sendGoResultMessage(gs, player)
                                .then(gameMessageSender.sendTurnInfo(gs, TURN_TIMEOUT_MILLIS));
                    } else {
                        return gamePlayService.gameOver(gs, player)
                                .delayUntil(finalState -> gameMessageSender.sendGameOverMessage(finalState, player));
                    }
                })
                .doOnNext(nextState -> {
                    if (nextState.getPhase() == IN_PROGRESS) {
                        autoPlayScheduler.scheduleAutoPlay(
                                roomId,
                                nextState.getRound(),
                                nextState.getCurrentTurn(),
                                nextState.getCurrentPlayer(),
                                nextDeadlineNanos(),
                                IN_PROGRESS
                        );
                    }
                })
                .then();
    }
}
