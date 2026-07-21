package com.pomingmatgo.gameservice.domain.messaging;

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
                WebSocketResDto.of(player, ResponseEvent.SUBMIT_CARD, "카드 제출", card)
        );
    }

    public Mono<Void> sendTopCardInfo(long roomId, Player player, Card card) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, ResponseEvent.CARD_REVEALED, "상단 카드 정보", card)
        );
    }

    public Mono<Void> sendAcquiredCardMessage(long roomId, Player player, List<Card> acquiredCards) {
        Map<CardType, List<Card>> classifiedCards = acquiredCards.stream()
                .collect(Collectors.groupingBy(Card::getType));

        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, ResponseEvent.ACQUIRED_CARD, "카드 획득", classifiedCards)
        );
    }

    public Mono<Void> sendTurnInfo(GameState gameState, long remainingMs) {
        AnnounceRoundRes res = new AnnounceRoundRes(
                gameState.getRound(),
                gameState.getCurrentTurn(),
                gameState.getCurrentPlayer(),
                remainingMs
        );
        return messageSender.sendMessageToAllUser(gameState.getRoomId(),
                WebSocketResDto.of(PLAYER_NOTHING, ResponseEvent.ANNOUNCE_TURN_INFORMATION, "턴을 알립니다.", res));
    }

    public Mono<Void> sendChooseFloorCardMessage(long roomId, Player player, List<Card> card) {
        WebSocketSession session = sessionManager.getSession(roomId, player.getNumber());
        return messageSender.sendMessageToSession(
                session,
                WebSocketResDto.of(player, ResponseEvent.CHOOSE_FLOOR_CARD, "바닥 카드 선택", card));
    }

    public Mono<Void> sendMovingCardMessage(long roomId, Player player, Player otherPlayer, Card card) {
        WebSocketSession session = sessionManager.getSession(roomId, player.getNumber());
        WebSocketSession otherSession = sessionManager.getSession(roomId, otherPlayer.getNumber());
        return messageSender.sendMessageToSession(
                session,
                WebSocketResDto.of(player, ResponseEvent.OPPONENT_PI_CLAIMED, "상대방의 카드를 빼았습니다.", card)).then(
                messageSender.sendMessageToSession(
                        otherSession,
                        WebSocketResDto.of(otherPlayer, ResponseEvent.OPPONENT_PI_CLAIMED, "상대방이 피를 뺏습니다.", card)
                ));
    }

    public Mono<Void> sendSpecialEventMessageIfNeeded(long roomId, Player player, SpecialEvent event) {
        if (event != SpecialEvent.NONE) {
            return messageSender.sendMessageToAllUser(
                    roomId,
                    WebSocketResDto.of(player, event.getResponseEvent(), event.getDisplayName())
            );
        }

        return Mono.empty();
    }

    public Mono<Void> sendScoreInfo(long roomId, ScoreInfoRes scoreInfo) {
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(PLAYER_NOTHING, ResponseEvent.SCORE_UPDATE, "점수 정보 업데이트", scoreInfo)
        );
    }

    public Mono<Void> sendGoStopChoiceMessage(GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        Player otherPlayer = gameState.getOtherPlayer();
        int nextGoNum = gameState.getPlayerState(player).getGo() + 1;
        WebSocketSession session = sessionManager.getSession(roomId, player.getNumber());
        WebSocketSession otherSession = sessionManager.getSession(roomId, otherPlayer.getNumber());
        return messageSender.sendMessageToSession(
                session,
                WebSocketResDto.of(player, ResponseEvent.GO_STOP_CHOICE, "고/스톱 선택", nextGoNum)
        ).then(
                messageSender.sendMessageToSession(
                        otherSession,
                        WebSocketResDto.of(otherPlayer, ResponseEvent.OPPONENT_GO_STOP_CHOICE, "상대방이 고/스톱을 선택합니다.", null)
                )
        );
    }

    public Mono<Void> sendGoResultMessage(GameState gameState, Player player) {
        long roomId = gameState.getRoomId();
        long goNum = gameState.getPlayerState(player).getGo();
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(player, ResponseEvent.GO_RESULT, "고 횟수", goNum)
        );
    }

    /** winner가 PLAYER_NOTHING이면 무승부 */
    public Mono<Void> sendGameOverMessage(GameState finalState, Player winner) {
        long roomId = finalState.getRoomId();
        String message = winner == PLAYER_NOTHING ? "무승부" : "게임 승리자";
        return messageSender.sendMessageToAllUser(
                roomId,
                WebSocketResDto.of(PLAYER_NOTHING, ResponseEvent.GAME_OVER, message, winner)
        );
    }
}
