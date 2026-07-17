package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.request.websocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.api.response.websocket.LeadSelectionRes;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import com.pomingmatgo.gameservice.scheduler.AutoPlayScheduler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.pomingmatgo.gameservice.domain.GamePhase.DETERMINING_STARTING_PLAYER;
import static com.pomingmatgo.gameservice.domain.Player.*;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;

@Component
@RequiredArgsConstructor
public class WsPreGameHandler {
    private final PreGameService preGameService;
    private final MessageSender messageSender;
    private final SessionManager sessionManager;
    private final AutoPlayScheduler autoPlayScheduler;
    private final TurnFlowService turnFlowService;

    public Mono<Void> handlePreGameEvent(RequestEvent<?> event, GameState gameState, Player player) {
        if(gameState.getPhase() != DETERMINING_STARTING_PLAYER) {
            return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
        }

        SubCategory eventType = SubCategory.from(event.getEventType().getSubType());

        return switch (eventType) {
            case LEADER_SELECTION -> handleLeaderSelectionEvent(event.as(), gameState, player);
            default -> Mono.error(new IllegalArgumentException("Invalid GAME event type"));
        };
    }

    private Mono<Void> handleLeaderSelectionEvent(RequestEvent<LeadSelectionReq> event, GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        int cardIndex = event.getData().cardIndex();

        return preGameService.selectLeaderCard(roomId, player, cardIndex)
                .then(preGameService.checkAllSelected(roomId))
                .flatMap(allSelected -> sendLeaderSelectionMessage(roomId, player, cardIndex)
                        .then(allSelected
                                ? afterleaderSelectionCardAllSelection(gameState)
                                : Mono.empty()));
    }

    private Mono<Void> sendLeaderSelectionMessage(long roomId, Player player, int cardIndex) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, "LEADER_SELECTION", "선두 플레이어 선택", cardIndex)
        );
    }

    private Mono<Void> afterleaderSelectionCardAllSelection(GameState gameState) {
        return finalizeLeaderSelection(gameState)
                .flatMap(this::distributeCardsAndNotify)
                .flatMap(this::checkChongtongAndProceed)
                .flatMap(this::startFirstTurn);
    }

    private Mono<GameState> finalizeLeaderSelection(GameState gameState) {
        Long roomId = gameState.getRoomId();
        return preGameService.getLeadSelectionRes(roomId)
                .flatMap(leadSelectionRes -> {
                    GameState updatedState = gameState.toBuilder()
                            .leadingPlayer(leadSelectionRes.getLeadPlayer())
                            .build();

                    return sendAllSelectedEvent(roomId, leadSelectionRes)
                            .thenReturn(updatedState);
                });
    }

    private Mono<GameState> distributeCardsAndNotify(GameState gameState) {
        Long roomId = gameState.getRoomId();
        return Mono.defer(() -> preGameService.distributeCards(roomId))
                .flatMap(cards -> sendDistributedCardInfo(roomId, cards))
                .thenReturn(gameState);
    }

    // todo: 승부판정 로직 구현 필요
    private Mono<GameState> checkChongtongAndProceed(GameState gameState) {
        Long roomId = gameState.getRoomId();

        Mono<Boolean> p1HasChongtong = preGameService.hasChongtong(roomId, PLAYER_1)
                .defaultIfEmpty(false);
        Mono<Boolean> p2HasChongtong = preGameService.hasChongtong(roomId, PLAYER_2)
                .defaultIfEmpty(false);

        return Mono.zip(p1HasChongtong, p2HasChongtong)
                .flatMap(tuple -> {
                    boolean p1Result = tuple.getT1();
                    boolean p2Result = tuple.getT2();

                    if (p1Result && p2Result) {
                        // 둘 다 총통인 경우 — todo: 무승부 처리
                        return Mono.just(gameState);
                    }
                    if (p1Result || p2Result) {
                        // todo: 총통 승부 처리
                        return Mono.just(gameState);
                    }
                    return Mono.just(gameState);
                });
    }

    private Mono<Void> startFirstTurn(GameState gameState) {
        // 첫 턴도 이후 턴 전환과 동일한 공지+자동플레이 타이머 등록 경로(TurnFlowService)를 탄다
        return preGameService.setFirstTurn(gameState)
                .flatMap(state -> turnFlowService.startTurn(state, autoPlayScheduler));
    }

    private Mono<Void> sendAllSelectedEvent(long roomId, LeadSelectionRes leadSelectionRes) {
        return messageSender.sendMessageToAllUser(roomId,
                WebSocketResDto.of(PLAYER_NOTHING, "LEADER_SELECTION_RESULT", "선을 정했습니다.", leadSelectionRes));
    }

    private Mono<Void> sendDistributedCardInfo(long roomId, InstalledCard installedCard) {
        WebSocketResDto<List<String>> ret1 =  WebSocketResDto.of(
                PLAYER_1,
                "DISTRIBUTE_CARD",
                "카드를 배분합니다.",
                installedCard.getPlayer1()
                        .stream()
                        .map(Enum::name)
                        .toList());

        WebSocketResDto<List<String>> ret2 =  WebSocketResDto.of(
                PLAYER_2,
                "DISTRIBUTE_CARD",
                "카드를 배분합니다.",
                installedCard.getPlayer2()
                        .stream()
                        .map(Enum::name)
                        .toList());

        Map<Integer, List<Card>> revealedCards = installedCard.getRevealedCard().stream()
                .collect(Collectors.groupingBy(Card::getMonth));

        WebSocketSession player1Session = sessionManager.getSession(roomId, 1);
        WebSocketSession player2Session = sessionManager.getSession(roomId, 2);

        return Mono.when(
                messageSender.sendMessageToSession(player1Session, ret1),
                messageSender.sendMessageToSession(player2Session, ret2),
                messageSender.sendMessageToAllUser(
                        roomId,
                        WebSocketResDto.of(PLAYER_NOTHING, "DISTRIBUTED_FLOOR_CARD", "바닥패 정보", revealedCards)
                )
        );
    }

}
