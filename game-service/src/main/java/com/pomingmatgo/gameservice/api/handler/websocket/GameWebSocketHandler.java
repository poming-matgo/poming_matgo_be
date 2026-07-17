package com.pomingmatgo.gameservice.api.handler.websocket;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.category.EventCategory;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.request.websocket.JoinRoomReq;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.ReconnectService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.WebSocketResDto;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.global.exception.dto.WebSocketErrorResDto;
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.*;


@Component
@RequiredArgsConstructor
@Slf4j
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final RoomService roomService;
    private final SessionManager sessionManager;
    private final WsRoomHandler wsRoomHandler;
    private final WsPreGameHandler wsPreGameHandler;
    private final WsGameHandler wsGameHandler;
    private final MessageSender messageSender;
    private final InFlightManager inFlightManager;
    private final RoomCleanupService roomCleanupService;
    private final ReconnectService reconnectService;

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> handleMessage(message, session))
                .then()
                // 정상 종료(onComplete) / 에러(onError) / 구독 취소(cancel) 모든 경로에서 disconnect 처리
                .doFinally(signal -> handleDisconnect(session)
                        .subscribeOn(Schedulers.boundedElastic())
                        .subscribe());
    }

    private Mono<Void> handleMessage(WebSocketMessage message, WebSocketSession session) {
        return Mono.<RequestEvent<?>>fromCallable(() -> {
                    JsonNode rootNode = objectMapper.readTree(message.getPayloadAsText());
                    String subTypeStr = rootNode.path("eventType").path("subType").asText();

                    SubCategory subType = SubCategory.from(subTypeStr);
                    JavaType type = objectMapper.getTypeFactory().constructParametricType(RequestEvent.class, subType.getPayloadClass());

                    RequestEvent<?> event = objectMapper.convertValue(rootNode, type);
                    event.setSubCategory(subType);
                    return event;
                })
                .flatMap(event -> processEvent(event, session))
                .onErrorResume(error -> handleWebSocketError(session, error));
    }

    private Mono<Void> processEvent(RequestEvent<?> event, WebSocketSession session) {
        if (event.getSubCategory() == SubCategory.CONNECT) {
            return handleJoinRoom(event.as(), session);
        }

        return sessionManager.getPlayerContext(session)
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_IN_ROOM)))
                .flatMap(context -> {
                    long roomId = context.roomId();
                    Player player = Player.fromNumber(context.playerNum());

                    return roomService.getGameState(roomId)
                            .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_EXISTED_ROOM)))
                            .flatMap(gameState -> {
                                if (isGameAction(event, gameState, player)) {
                                    // 정상 요청은 NORMAL 키만 사용. 자동플레이가 진행 중이어도 InFlight 단계에선 차단되지 않음.
                                    // 자동플레이의 abort는 routeEvent 안의 onLockAcquired 콜백에서 cancelAutoPlay 호출로 처리됨.
                                    String flagKey = InFlightManager.normalKey(roomId, player.getNumber());
                                    // 요청별 소유 토큰: TTL 만료 후 다른 요청이 플래그를 재획득해도 내 정리가 남의 플래그를 지우지 않게 함
                                    String flagToken = Long.toHexString(ThreadLocalRandom.current().nextLong());

                                    return inFlightManager.trySetFlag(flagKey, flagToken, Duration.ofSeconds(3))
                                            .flatMap(isSet -> {
                                                if (!isSet) return Mono.error(new WebSocketBusinessException(TOO_MANY_REQUESTS));
                                                return Mono.usingWhen(
                                                        Mono.just(flagKey),
                                                        key -> routeEvent(event, gameState, player),
                                                        key -> inFlightManager.deleteFlag(key, flagToken)
                                                );
                                            });
                                }

                                return routeEvent(event, gameState, player);
                            });
                });
    }

    private boolean isGameAction(RequestEvent<?> event, GameState gameState, Player player) {
        SubCategory eventType = event.getSubCategory();

        boolean cond1 =  eventType == SubCategory.NORMAL_SUBMIT ||
               eventType == SubCategory.FLOOR_SELECT ||
               eventType == SubCategory.GO_STOP_CHOICE;

        // 행동 대기 phase(제출/바닥 선택/고스톱 선택)는 모두 자동플레이 타이머와 경합하므로 InFlight 방어가 필요하다
        boolean cond2 = gameState.getPhase().isPlayerActionPhase() &&
                player.equals(gameState.getCurrentPlayer());

        return cond1 && cond2;
    }

    private Mono<Void> handleJoinRoom(RequestEvent<JoinRoomReq> event, WebSocketSession session) {
        return sessionManager.getPlayerContext(session)
                .hasElement()
                .flatMap(isJoined -> isJoined
                        ? Mono.error(new WebSocketBusinessException(ALREADY_JOIN))
                        : processJoinRoomLogic(event.getData(), session));
    }

    private Mono<Void> processJoinRoomLogic(JoinRoomReq payload, WebSocketSession session) {
        long userId = payload.userId();
        long roomId = payload.roomId();

        return roomService.getGameState(roomId)
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_EXISTED_ROOM)))
                .flatMap(gameState -> Mono.fromCallable(() -> gameState.getPlayerType(userId))
                        .flatMap(player -> sessionManager.addPlayer(roomId, player, userId, session)
                                // 행동 대기 phase의 CONNECT는 진행 중인 게임으로의 재접속
                                .then(gameState.getPhase().isPlayerActionPhase()
                                        ? handleReconnect(roomId, player, session)
                                        : messageSender.sendMessageToAllUser(
                                                roomId, WebSocketResDto.of(player, "CONNECT", "접속했습니다.")))));
    }

    /**
     * 진행 중인 게임으로의 재접속: 양쪽에 재접속을 알리고, 재접속자에게 화면 복원용 상태 스냅샷을 보낸다.
     * 스냅샷은 fresh 조회 — 이탈 중 자동플레이가 게임을 진행시켰을 수 있다.
     */
    private Mono<Void> handleReconnect(long roomId, Player player, WebSocketSession session) {
        return messageSender.sendMessageToAllUser(
                        roomId, WebSocketResDto.of(player, "RECONNECT", "재접속했습니다."))
                .then(reconnectService.buildSnapshot(roomId, player))
                .flatMap(snapshot -> messageSender.sendMessageToSession(
                        session, WebSocketResDto.of(player, "RECONNECT_STATE", "재접속 상태 동기화", snapshot)));
    }

    private Mono<Void> handleWebSocketError(WebSocketSession session, Throwable error) {
        boolean isSystemError = true;
        WebSocketErrorCode errorCode = SYSTEM_ERROR;

        if (error instanceof WebSocketBusinessException wbe) {
            errorCode = wbe.getWebsocketErrorCode();
            isSystemError = (errorCode == SYSTEM_ERROR);
        }

        if (isSystemError) {
            log.error("WebSocket system error occurred in session [{}].", session.getId(), error);
        }

        WebSocketErrorResDto errorDto = new WebSocketErrorResDto(errorCode);

        if (!session.isOpen()) {
            return Mono.empty();
        }
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(errorDto))
                .map(session::textMessage)
                .flatMap(message -> session.send(Mono.just(message)))
                .onErrorResume(sendError -> {
                    log.debug("에러 응답 전송 실패 — 세션 [{}] 스킵", session.getId(), sendError);
                    return Mono.empty();
                });
    }

    private Mono<Void> handleDisconnect(WebSocketSession session) {
        return sessionManager.getPlayerContext(session)
                .flatMap(context -> {
                    long roomId = context.roomId();
                    int playerNum = context.playerNum();
                    Player disconnected = Player.fromNumber(playerNum);

                    // identity guard: 컨텍스트 조회 후 재접속이 슬롯을 교체했다면 새 세션을 지우지 않는다
                    sessionManager.deletePlayer(roomId, playerNum, session);

                    // 슬롯이 다시 점유돼 있다면 이 disconnect는 재접속에 밀린 낡은 것 —
                    // 보존/teardown 판정까지 진행하면 방금 재접속한 세션 밑에서
                    // OPPONENT_DISCONNECTED 오발송이나 방 파괴가 일어나므로 통째로 중단한다
                    if (sessionManager.getSession(roomId, playerNum) != null) {
                        return Mono.empty();
                    }

                    return roomService.getGameState(roomId)
                            // 방 상태가 이미 없으면(teardown과 교차한 재접속 등) 세션 매핑만 마저 정리 (roomSessions 누수 방지)
                            .switchIfEmpty(Mono.defer(() ->
                                    sessionManager.removeRoom(roomId).then(Mono.<GameState>empty())))
                            .flatMap(gameState -> {
                                GamePhase phase = gameState.getPhase();
                                boolean opponentConnected =
                                        sessionManager.getSession(roomId, disconnected.opponent().getNumber()) != null;

                                // 행동 대기 phase는 자동플레이 타이머가 진행(liveness)을 보장하므로
                                // 방을 보존해 재접속을 허용한다 — 이탈자의 턴은 기존 타이머가 그대로 대행.
                                // 단, 마지막 접속자까지 나가면 버려진 방이므로 즉시 정리한다.
                                if (phase.isPlayerActionPhase() && opponentConnected) {
                                    return messageSender.sendMessageToAllUser(roomId,
                                            WebSocketResDto.of(disconnected, "OPPONENT_DISCONNECTED",
                                                    "상대방의 연결이 끊겼습니다. 재접속할 때까지 자동플레이로 진행합니다."));
                                }

                                boolean inProgress = phase != GamePhase.NONE && phase != GamePhase.END;

                                Mono<Void> notify = inProgress
                                        ? messageSender.sendMessageToAllUser(roomId,
                                                WebSocketResDto.of(disconnected, "OPPONENT_DISCONNECTED",
                                                        "상대방이 연결을 끊어 게임이 종료됩니다."))
                                        : Mono.empty();

                                return notify
                                        .then(roomCleanupService.cleanupRoomData(roomId))
                                        .then(sessionManager.removeRoom(roomId));
                            });
                })
                .onErrorResume(e -> {
                    log.warn("Disconnect handling failed for session [{}]", session.getId(), e);
                    return Mono.empty();
                });
    }

    private Mono<Void> routeEvent(RequestEvent<?> event, GameState gameState, Player player) {
        EventCategory eventType = EventCategory.valueOf(event.getEventType().getType());

        return switch (eventType) {
            case ROOM -> wsRoomHandler.handleRoomEvent(event, gameState, player);
            case PREGAME -> wsPreGameHandler.handlePreGameEvent(event, gameState, player);
            case GAME -> wsGameHandler.handleGameEvent(event, gameState, player);
            default -> Mono.empty();
        };
    }
}
