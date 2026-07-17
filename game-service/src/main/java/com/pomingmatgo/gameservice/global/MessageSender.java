package com.pomingmatgo.gameservice.global;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class MessageSender {
    private final ObjectMapper objectMapper;
    private final SessionManager sessionManager;
    // metrics.throughput.enabled=false면 bean이 없어 null — hot path라 기동 시 1회만 조회해 둔다
    private final ThroughputRecorder throughputRecorder;

    public MessageSender(ObjectMapper objectMapper,
                         SessionManager sessionManager,
                         ObjectProvider<ThroughputRecorder> throughputRecorderProvider) {
        this.objectMapper = objectMapper;
        this.sessionManager = sessionManager;
        this.throughputRecorder = throughputRecorderProvider.getIfAvailable();
    }

    public <T> Mono<Void> sendMessageToSession(WebSocketSession session, WebSocketResDto<T> response) {
        // session null: 상대 미접속 또는 방 정리와 동시 실행된 경우 → 전송 스킵
        if (session == null || !session.isOpen()) {
            return Mono.empty();
        }

        return Mono.fromCallable(() -> objectMapper.writeValueAsString(response))
                .map(session::textMessage)
                .flatMap(msg -> session.send(Mono.just(msg)))
                // 전송 성공만 계측 — skip(null/closed 세션)·실패는 throughput에 포함하지 않는다
                .doOnSuccess(v -> {
                    if (throughputRecorder != null) {
                        throughputRecorder.recordSent();
                    }
                })
                // 전송 실패는 게임 진행을 막지 않는다 — 세션 사망은 disconnect 처리가 별도로 감지·수습
                .onErrorResume(e -> {
                    log.debug("WS 메시지 전송 실패 — 세션 [{}] 스킵", session.getId(), e);
                    return Mono.empty();
                });
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
