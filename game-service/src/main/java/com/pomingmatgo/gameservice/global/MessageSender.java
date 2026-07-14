package com.pomingmatgo.gameservice.global;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MessageSender {
    private final ObjectMapper objectMapper;
    private final SessionManager sessionManager;
    public <T> Mono<Void> sendMessageToSession(WebSocketSession session, WebSocketResDto<T> response) {
        //todo: 상세 예외처리 필요
        // session null: 상대 미접속 또는 방 정리와 동시 실행된 경우 → 전송 스킵
        if (session == null || !session.isOpen()) {
            return Mono.empty();
        }

        return Mono.fromCallable(() -> objectMapper.writeValueAsString(response))
                .map(session::textMessage)
                .flatMap(msg -> session.send(Mono.just(msg)))
                .onErrorResume(e -> Mono.empty());
    }

    public <T> Mono<Void> sendMessageToAllUser(long roomId, WebSocketResDto<T> response) {
        // 수신자 조회는 구독 시점으로 지연(defer)해야 한다.
        // assembly 시점에 getAllUser를 즉시 평가하면 addPlayer(...).then(broadcast) 체인에서
        // 세션 등록 전에 수신자를 캡처해 접속 직후 첫 브로드캐스트가 유실된다 (CONNECT ack 미도달 버그).
        return Flux.defer(() -> Flux.fromIterable(sessionManager.getAllUser(roomId)))
                .flatMap(session -> sendMessageToSession(session, response))
                .then();
    }
}
