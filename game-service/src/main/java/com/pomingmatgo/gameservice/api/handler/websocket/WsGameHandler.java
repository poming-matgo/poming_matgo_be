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
import org.redisson.api.RedissonReactiveClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.domain.GamePhase.*;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.NOT_YOUR_TURN;

@Component
@RequiredArgsConstructor
@Slf4j
public class WsGameHandler {

    private final GamePlayService gamePlayService;
    private final GameMessageSender gameMessageSender;
    private final GameNotificationService gameNotificationService;
    private final AutoPlayScheduler autoPlayScheduler;
    private final RedissonReactiveClient redissonReactiveClient;

    long TURN_TIMEOUT_MILLIS = 10000L;

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
        if(gameState.getPhase() != IN_PROGRESS) {
            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
        }

        long roomId = gameState.getRoomId();
        int cardIdx = event.getData().cardIndex();

        String flagKey = "IN_FLIGHT:ROOM:" + roomId;

        Mono<Void> mainProcess =  gamePlayService.executeNormalSubmit(roomId, gameState, player, cardIdx, () -> autoPlayScheduler.cancelAutoPlay(roomId))
                .flatMap(ctx -> {
                    Mono<Void> sendInfos = Mono.when(
                            gameMessageSender.sendSubmitCardInfo(roomId, player, ctx.submittedCard()),
                            gameMessageSender.sendTopCardInfo(roomId, player, ctx.topCard())
                    );

                    long deadlineMillis = System.currentTimeMillis() + TURN_TIMEOUT_MILLIS;

                    Mono<Void> handleResult;
                    if (ctx.isChoiceRequired()) {
                        autoPlayScheduler.cancelAutoPlay(roomId);
                        handleResult = gameMessageSender.sendChooseFloorCardMessage(roomId, player, ctx.cardResult().getSelectableCards());
                    } else {
                        handleResult = gameNotificationService.broadcastTurnResult(roomId, player, ctx.updatedGameState(), ctx.cardResult(), () -> autoPlayScheduler.cancelAutoPlay(roomId))
                                .doOnNext(nextState -> {
                                    if (nextState.getPhase() == IN_PROGRESS) {
                                        log.info("[AutoPlay Schedule] 스케줄링 등록! roomId: {}, 예약된 턴: ({}, {}), 대상 플레이어: {}",
                                                roomId, nextState.getRound(), nextState.getCurrentTurn(), nextState.getCurrentPlayer());
                                        autoPlayScheduler.scheduleAutoPlay(
                                                roomId,
                                                nextState.getRound(),
                                                nextState.getCurrentTurn(),
                                                nextState.getCurrentPlayer(),
                                                deadlineMillis
                                        );
                                    }
                                })
                                .then();
                    }

                    return sendInfos.then(handleResult);
                });

        return Mono.usingWhen(
                Mono.just(flagKey),
                key -> mainProcess,
                key -> redissonReactiveClient.getBucket(key).delete()
        ).then();
    }

    private Mono<Void> handleFloorSelect(RequestEvent<NormalSubmitReq> event, GameState gameState, Player player) {
        if(gameState.getPhase() != AWAITING_FLOOR_CARD_CHOICE) {
            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
        }
        long roomId = gameState.getRoomId();

        return gamePlayService.executeFloorSelection(roomId, gameState, player, event, () -> autoPlayScheduler.cancelAutoPlay(roomId))
                .flatMap(ctx -> {
                    if (ctx.isChoiceRequired()) {
                        return gameMessageSender.sendChooseFloorCardMessage(roomId, player, ctx.cardResult().getSelectableCards());
                    }

                    Mono<Void> sendAcquired = gameMessageSender.sendAcquiredCardMessage(roomId, player, ctx.cardResult().getAcquiredCards());
                    Mono<GameState> setNextTurn = gamePlayService.proceedToNextTurn(ctx.updatedGameState());

                    long deadlineMillis = System.currentTimeMillis() + TURN_TIMEOUT_MILLIS;

                    return sendAcquired.then(setNextTurn)
                            .delayUntil(gameNotificationService::broadcastNextTurnInfo)
                            .doOnNext(nextState -> {
                                if (nextState.getPhase() == IN_PROGRESS) {
                                    log.info("[AutoPlay Schedule] 스케줄링 등록! roomId: {}, 예약된 턴: ({}, {}), 대상 플레이어: {}",
                                            roomId, nextState.getRound(), nextState.getCurrentTurn(), nextState.getCurrentPlayer());
                                    autoPlayScheduler.scheduleAutoPlay(
                                            roomId,
                                            nextState.getRound(),
                                            nextState.getCurrentTurn(),
                                            nextState.getCurrentPlayer(),
                                            deadlineMillis
                                    );
                                }
                            })
                            .then();


                });
    }

    private Mono<Void> handleGoStopChoice(RequestEvent<GoStopReq> event, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        long deadlineMillis = System.currentTimeMillis() + TURN_TIMEOUT_MILLIS;
        return gamePlayService.executeGoStop(gameState, player, event, () -> autoPlayScheduler.cancelAutoPlay(roomId))
                .delayUntil(gs -> {
                    if (gs.isPlaying()) {
                        return gameMessageSender.sendGoResultMessage(gs, player)
                                .then(gameMessageSender.sendTurnInfo(gs));
                    } else {
                        return Mono.empty();
                    }
                })
                .doOnNext(nextState -> {
                    if (nextState.getPhase() == IN_PROGRESS) {
                        log.info("[AutoPlay Schedule] 스케줄링 등록! roomId: {}, 예약된 턴: ({}, {}), 대상 플레이어: {}",
                                roomId, nextState.getRound(), nextState.getCurrentTurn(), nextState.getCurrentPlayer());
                        autoPlayScheduler.scheduleAutoPlay(
                                roomId,
                                nextState.getRound(),
                                nextState.getCurrentTurn(),
                                nextState.getCurrentPlayer(),
                                deadlineMillis
                        );
                    }
                })
                .then();
    }
}
