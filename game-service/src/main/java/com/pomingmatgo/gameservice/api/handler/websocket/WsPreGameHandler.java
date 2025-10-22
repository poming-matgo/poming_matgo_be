package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.api.response.websocket.AnnounceRoundRes;
import com.pomingmatgo.gameservice.api.response.websocket.LeadSelectionRes;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@RequiredArgsConstructor
public class WsPreGameHandler {
    private final PreGameService preGameService;
    private final MessageSender messageSender;
    private final SessionManager sessionManager;

    private enum PreGameEventType {
        LEADER_SELECTION
    }

    public Mono<Void> handlePreGameEvent(RequestEvent<?> event, GameState gameState, int playerNum) {
        WsPreGameHandler.PreGameEventType eventType;
        try {
            eventType = WsPreGameHandler.PreGameEventType.valueOf(event.getEventType().getSubType());
        } catch (IllegalArgumentException e) {
            return Mono.error(new IllegalArgumentException("Unsupported event type: " + event.getEventType().getSubType()));
        }

        return switch (eventType) {
            case LEADER_SELECTION -> handleLeaderSelectionEvent(event, gameState, playerNum);
        };
    }

    private Mono<Void> handleLeaderSelectionEvent(RequestEvent<?> event, GameState gameState, int playerNum) {
        long roomId = gameState.getRoomId();
        return preGameService.selectCard((RequestEvent<LeadSelectionReq>) event)
                .then(sendLeaderSelectionMessage(roomId, playerNum))
                .then(preGameService.isAllPlayerCardSelected(roomId))
                .flatMap(allSelected -> {
                    if (Boolean.TRUE.equals(allSelected)) {
                        return afterleaderSelectionCardAllSelection(gameState);
                    }
                    return Mono.empty();
                });
    }

    private Mono<Void> sendLeaderSelectionMessage(long roomId, int playerNum) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(playerNum, "LEADER_SELECTION", "선두 플레이어 선택")
        );
    }

    private Mono<Void> afterleaderSelectionCardAllSelection(GameState gameState) {
        Long roomId = gameState.getRoomId();
        return preGameService.getLeadSelectionRes(roomId)
                .flatMap(leadSelectionRes -> {
                    GameState.GameStateBuilder builder = gameState.toBuilder();
                    builder.leadingPlayer(leadSelectionRes.getLeadPlayer());
                    return sendAllSelectedEvent(roomId, leadSelectionRes);
                })
                .then(Mono.defer(() -> preGameService.distributeCards(roomId)))
                .flatMap(cards -> sendDistributedCardInfo(roomId, cards)
                        .then(preGameService.setRoundInfo(gameState))
                        .flatMap(this::announceTurnToAllPlayers));
    }

    private Mono<Void> sendAllSelectedEvent(long roomId, LeadSelectionRes leadSelectionRes) {
        return messageSender.sendMessageToAllUser(roomId,
                WebSocketResDto.of(0, "LEADER_SELECTION_RESULT", "선을 정했습니다.", leadSelectionRes));
    }

    private Mono<Void> sendDistributedCardInfo(long roomId, InstalledCard installedCard) {
        WebSocketResDto<List<String>> ret1 =  WebSocketResDto.of(
                1,
                "DISTRIBUTE_CARD",
                "카드를 배분합니다.",
                installedCard.getPlayer1()
                        .stream()
                        .map(Enum::name)
                        .toList());

        WebSocketResDto<List<String>> ret2 =  WebSocketResDto.of(
                2,
                "DISTRIBUTE_CARD",
                "카드를 배분합니다.",
                installedCard.getPlayer2()
                        .stream()
                        .map(Enum::name)
                        .toList());

        WebSocketSession player1Session = sessionManager.getSession(roomId, 1);
        WebSocketSession player2Session = sessionManager.getSession(roomId, 2);

        return Mono.when(
                messageSender.sendMessageToSession(player1Session, ret1),
                messageSender.sendMessageToSession(player2Session, ret2)
        );
    }

    private Mono<Void> announceTurnToAllPlayers(GameState gameState) {
        AnnounceRoundRes res = new AnnounceRoundRes(
                gameState.getRound(),
                gameState.getCurrentTurn(),
                gameState.getLeadingPlayer()==gameState.getCurrentTurn() ? 1 : 2
        );
        return messageSender.sendMessageToAllUser(gameState.getRoomId(),
                WebSocketResDto.of(0, "ANNOUNCE_TURN_INFORMATION", "턴을 알립니다.", res));
    }

}
