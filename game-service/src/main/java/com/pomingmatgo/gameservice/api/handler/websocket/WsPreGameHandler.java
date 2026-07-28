package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameFlowService;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.domain.GamePhase.DETERMINING_STARTING_PLAYER;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;

@Component
@RequiredArgsConstructor
public class WsPreGameHandler {
    private final PreGameFlowService preGameFlowService;

    public Mono<Void> handlePreGameEvent(RequestEvent<?> event, GameState gameState, Player player) {
        if(gameState.getPhase() != DETERMINING_STARTING_PLAYER) {
            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
        }

        return switch (event.getSubCategory()) {
            case LEADER_SELECTION -> handleLeaderSelectionEvent(event.as(), gameState, player);
            default -> Mono.error(new IllegalStateException("처리기가 없는 PREGAME 이벤트: " + event.getSubCategory()));
        };
    }

    private Mono<Void> handleLeaderSelectionEvent(RequestEvent<LeadSelectionReq> event, GameState gameState, Player player) {
        return preGameFlowService.processLeaderSelection(gameState, player, event.getData().cardIndex());
    }
}
