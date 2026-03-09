package com.pomingmatgo.gameservice.api.response.websocket;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.card.CardType;
import com.pomingmatgo.gameservice.domain.service.matgo.SpecialEvent;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_NOTHING;

@Component
@RequiredArgsConstructor
public class GameMessageSender {
    private final MessageSender messageSender;
    private final SessionManager sessionManager;



    public Mono<Void> sendSubmitCardInfo(long roomId, Player player, Card card) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, "SUBMIT_CARD", "카드 제출", card)
        );
    }

    public Mono<Void> sendTopCardInfo(long roomId, Player player, Card card) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, "CARD_REVEALED", "상단 카드 정보", card)
        );
    }

    public Mono<Void> sendAcquiredCardMessage(long roomId, Player player, List<Card> acquiredCards) {
        Map<CardType, List<Card>> classifiedCards = acquiredCards.stream()
                .collect(Collectors.groupingBy(Card::getType));

        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, "ACQUIRED_CARD", "카드 획득", classifiedCards)
        );
    }

    public Mono<Void> sendTurnInfo(GameState gameState) {
        AnnounceRoundRes res = new AnnounceRoundRes(
                gameState.getRound(),
                gameState.getCurrentTurn(),
                gameState.getCurrentPlayer()
        );
        return messageSender.sendMessageToAllUser(gameState.getRoomId(),
                WebSocketResDto.of(PLAYER_NOTHING, "ANNOUNCE_TURN_INFORMATION", "턴을 알립니다.", res));
    }

    public Mono<Void> sendChooseFloorCardMessage(long roomId, Player player, List<Card> card) {
        WebSocketSession session = sessionManager.getSession(roomId, player.getNumber());
        return messageSender.sendMessageToSession(
                session,
                WebSocketResDto.of(player, "CHOOSE_FLOOR_CARD", "바닥 카드 선택", card));
    }

    //뺏는것과 빼앗는것 구분 필요
    public Mono<Void> sendMovingCardMessage(long roomId, Player player, Player otherPlayer, Card card) {
        WebSocketSession session = sessionManager.getSession(roomId, player.getNumber());
        WebSocketSession otherSession = sessionManager.getSession(roomId, otherPlayer.getNumber());
        return messageSender.sendMessageToSession(
                session,
                WebSocketResDto.of(player, "OPPONENT_PI_CLAIMED", "상대방의 카드를 빼았습니다.", card)).then(
                messageSender.sendMessageToSession(
                        otherSession,
                        WebSocketResDto.of(otherPlayer, "OPPONENT_PI_CLAIMED", "상대방이 피를 뺏습니다.", card)
                ));
    }

    public Mono<Void> sendSpecialEventMessageIfNeeded(long roomId, Player player, SpecialEvent event) {
        if (event != SpecialEvent.NONE) {
            return messageSender.sendMessageToAllUser(
                    roomId,
                    WebSocketResDto.of(player, event.name(), event.getDisplayName())
            );
        }

        // NONE이거나 처리할 게 없으면 빈 Mono 반환
        return Mono.empty();
    }

    public Mono<Void> sendScoreInfo(long roomId, ScoreInfoRes scoreInfo) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(PLAYER_NOTHING, "SCORE_UPDATE", "점수 정보 업데이트", scoreInfo)
        );
    }

    public Mono<Void> sendGoStopChoiceMessage(long roomId, Player player) {
        WebSocketSession session = sessionManager.getSession(roomId, player.getNumber());
        return messageSender.sendMessageToSession(
                session,
                WebSocketResDto.of(player, "GO_STOP_CHOICE", "고/스톱 선택", null)
        );
    }
}
