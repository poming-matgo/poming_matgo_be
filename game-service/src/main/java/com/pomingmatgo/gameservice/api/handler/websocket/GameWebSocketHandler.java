package com.pomingmatgo.gameservice.api.handler.websocket;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.RequestEventDecoder;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.request.websocket.JoinRoomReq;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.global.exception.dto.WebSocketErrorResDto;
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import com.pomingmatgo.gameservice.global.session.GameConnectionService;
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
    private final RequestEventDecoder eventDecoder;
    private final GameService gameService;
    private final SessionManager sessionManager;
    private final WsRoomHandler wsRoomHandler;
    private final WsPreGameHandler wsPreGameHandler;
    private final WsGameHandler wsGameHandler;
    private final MessageSender messageSender;
    private final InFlightManager inFlightManager;
    private final GameConnectionService connectionService;

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                // 세션 내 순차 처리 — demand가 Netty까지 전파돼 flood 클라이언트를 TCP 레벨에서 차단
                .concatMap(message -> handleMessage(message, session))
                .then()
                // 정상 종료(onComplete) / 에러(onError) / 구독 취소(cancel) 모든 경로에서 disconnect 처리
                .doFinally(signal -> connectionService.disconnect(session)
                        .subscribeOn(Schedulers.boundedElastic())
                        .subscribe());
    }

    private Mono<Void> handleMessage(WebSocketMessage message, WebSocketSession session) {
        return Mono.defer(() -> eventDecoder.decode(message.getPayloadAsText()))
                .flatMap(event -> processEvent(event, session))
                .onErrorResume(error -> handleWebSocketError(session, error));
    }

    private Mono<Void> processEvent(RequestEvent<?> event, WebSocketSession session) {
        if (event.getSubCategory() == SubCategory.CONNECT) {
            RequestEvent<JoinRoomReq> connectEvent = event.as();
            JoinRoomReq payload = connectEvent.getData();
            return connectionService.connect(payload.roomId(), payload.userId(), session);
        }

        return sessionManager.getPlayerContext(session)
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_IN_ROOM)))
                .flatMap(context -> {
                    long roomId = context.roomId();
                    Player player = Player.fromNumber(context.playerNum());

                    return gameService.findGameState(roomId)
                            .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_EXISTED_ROOM)))
                            .flatMap(gameState -> {
                                if (isGameAction(event, gameState, player)) {
                                    // NORMAL 키만 사용 — 대기 타이머 취소는 게임 액션 성공 콜백이 맡는다
                                    String flagKey = InFlightManager.normalKey(roomId, player.getNumber());
                                    // 요청별 소유 토큰 — TTL 만료 후 다른 요청이 재획득해도 내 정리가 남의 플래그를 지우지 않는다
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

        // 행동 대기 phase는 모두 자동플레이 타이머와 경합하므로 InFlight 방어 대상이다
        boolean cond2 = gameState.getPhase().isPlayerActionPhase() &&
                player.equals(gameState.getCurrentPlayer());

        return cond1 && cond2;
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

        return messageSender.sendPayload(session, new WebSocketErrorResDto(errorCode));
    }

    private Mono<Void> routeEvent(RequestEvent<?> event, GameState gameState, Player player) {
        // SubCategory가 카테고리를 유일 결정 — 클라가 보낸 type 문자열은 라우팅에 쓰지 않는다
        return switch (event.getSubCategory().getCategory()) {
            case ROOM -> wsRoomHandler.handleRoomEvent(event, gameState, player);
            case PREGAME -> wsPreGameHandler.handlePreGameEvent(event, gameState, player);
            case GAME -> wsGameHandler.handleGameEvent(event, gameState, player);
        };
    }
}
