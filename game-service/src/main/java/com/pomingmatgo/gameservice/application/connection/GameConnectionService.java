package com.pomingmatgo.gameservice.application.connection;

import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.messaging.ResponseEvent;
import com.pomingmatgo.gameservice.application.game.GameService;
import com.pomingmatgo.gameservice.application.connection.ReconnectService;
import com.pomingmatgo.gameservice.application.room.RoomCleanupService;
import com.pomingmatgo.gameservice.infrastructure.messaging.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class GameConnectionService {
    private final GameService gameService;
    private final SessionManager sessionManager;
    private final MessageSender messageSender;
    private final RoomCleanupService roomCleanupService;
    private final ReconnectService reconnectService;

    public Mono<Void> connect(Long roomId, Long userId, WebSocketSession session) {
        return sessionManager.getPlayerContext(session)
                .hasElement()
                .flatMap(isJoined -> isJoined
                        ? Mono.error(new WebSocketBusinessException(ALREADY_JOIN))
                        : connectToRoom(roomId, userId, session));
    }

    private Mono<Void> connectToRoom(Long roomId, Long userId, WebSocketSession session) {
        if (roomId == null || userId == null) {
            return Mono.error(new WebSocketBusinessException(INVALID_REQUEST));
        }

        return gameService.findGameState(roomId)
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_EXISTED_ROOM)))
                .flatMap(gameState -> Mono.fromCallable(() -> gameState.getPlayerType(userId))
                        .flatMap(player -> sessionManager.addPlayer(roomId, player, userId, session)
                                // 행동 대기 phase의 CONNECT는 진행 중인 게임으로의 재접속
                                .then(gameState.getPhase().isPlayerActionPhase()
                                        ? handleReconnect(roomId, player, session)
                                        : messageSender.sendMessageToAllUser(
                                                roomId, WebSocketResDto.of(player, ResponseEvent.CONNECT, "접속했습니다.")))));
    }

    private Mono<Void> handleReconnect(long roomId, Player player, WebSocketSession session) {
        return messageSender.sendMessageToAllUser(
                        roomId, WebSocketResDto.of(player, ResponseEvent.RECONNECT, "재접속했습니다."))
                .then(reconnectService.buildSnapshot(roomId, player))
                .flatMap(snapshot -> messageSender.sendMessageToSession(
                        session, WebSocketResDto.of(player, ResponseEvent.RECONNECT_STATE, "재접속 상태 동기화", snapshot)));
    }

    public Mono<Void> disconnect(WebSocketSession session) {
        return sessionManager.getPlayerContext(session)
                .flatMap(context -> {
                    long roomId = context.roomId();
                    int playerNum = context.playerNum();
                    Player disconnected = Player.fromNumber(playerNum);

                    // identity guard: 컨텍스트 조회 후 재접속이 슬롯을 교체했다면 새 세션을 지우지 않는다
                    sessionManager.deletePlayer(roomId, playerNum, session);

                    // 슬롯이 다시 점유됐다면 재접속에 밀린 낡은 disconnect —
                    // 계속 진행하면 방금 재접속한 세션 밑에서 오알림·방 파괴가 일어난다
                    if (sessionManager.getSession(roomId, playerNum) != null) {
                        return Mono.empty();
                    }

                    return gameService.findGameState(roomId)
                            // 방 상태가 이미 없으면 세션 매핑만 마저 정리 (roomSessions 누수 방지)
                            .switchIfEmpty(Mono.defer(() ->
                                    sessionManager.removeRoom(roomId).then(Mono.<GameState>empty())))
                            .flatMap(gameState -> {
                                GamePhase phase = gameState.getPhase();
                                boolean opponentConnected =
                                        sessionManager.getSession(roomId, disconnected.opponent().getNumber()) != null;

                                // 행동 대기 phase만 자동플레이가 진행(liveness)을 보장하므로 방을 보존한다.
                                // 마지막 접속자까지 나가면 버려진 방이라 아래로 흘려보내 정리한다
                                if (phase.isPlayerActionPhase() && opponentConnected) {
                                    return messageSender.sendMessageToAllUser(roomId,
                                            WebSocketResDto.of(disconnected, ResponseEvent.OPPONENT_DISCONNECTED,
                                                    "상대방의 연결이 끊겼습니다. 재접속할 때까지 자동플레이로 진행합니다."));
                                }

                                boolean inProgress = phase != GamePhase.NONE && phase != GamePhase.END;

                                Mono<Void> notify = inProgress
                                        ? Mono.defer(() -> messageSender.sendMessageToAllUser(roomId,
                                                WebSocketResDto.of(disconnected, ResponseEvent.OPPONENT_DISCONNECTED,
                                                        "상대방이 연결을 끊어 게임이 종료됩니다.")))
                                        : Mono.empty();

                                // 종료 안내부터 방 정리가 소유하므로 연결 취소가 정리 진입을 막지 않는다.
                                return roomCleanupService.cleanupRoom(roomId, notify);
                            });
                })
                .onErrorResume(e -> {
                    log.warn("Disconnect handling failed for session [{}]", session.getId(), e);
                    return Mono.empty();
                });
    }
}
