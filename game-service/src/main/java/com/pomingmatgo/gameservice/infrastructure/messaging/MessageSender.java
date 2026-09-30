package com.pomingmatgo.gameservice.infrastructure.messaging;

import com.pomingmatgo.gameservice.global.WebSocketResDto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import com.pomingmatgo.gameservice.infrastructure.session.SessionManager;
import com.pomingmatgo.gameservice.infrastructure.session.SnapshotDelivery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.util.function.Function;
import java.util.function.UnaryOperator;

@Component
@Slf4j
public class MessageSender {
    private record Recipients(Function<WebSocketSession, Mono<Boolean>> allows) {}
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
        return sendPayload(session, response);
    }

    /** 게임 락 안에서 캡처하고, 해당 액션의 락 밖 안내 전체에 적용한다. */
    public UnaryOperator<Context> captureRecipients(long roomId) {
        Recipients recipients = new Recipients(sessionManager.captureRecipients(roomId));
        return context -> context.put(Recipients.class, recipients);
    }

    public Mono<Void> sendPayload(WebSocketSession session, Object payload) {
        // 미구독 호출은 집계하지 않으며, 연결 상태도 실제 송신 구독 시점에 확인한다.
        return Mono.deferContextual(context -> {
            Recipients recipients = context.getOrDefault(Recipients.class, null);
            Mono<Boolean> permission = recipients == null ? Mono.just(true) : recipients.allows().apply(session);
            return permission.flatMap(allowed -> sendPayloadResult(session, payload, allowed)).then();
        });
    }

    public <T> Mono<Void> sendSnapshotToSession(WebSocketSession session, WebSocketResDto<T> response,
                                               SnapshotDelivery delivery) {
        return sendPayloadResult(session, response, true)
                .doOnNext(delivery::complete)
                .flatMap(sent -> sent ? Mono.<Void>empty()
                        : Mono.error(new IllegalStateException("Reconnect snapshot send failed")));
    }

    private Mono<Boolean> sendPayloadResult(WebSocketSession session, Object payload, boolean allowed) {
        return Mono.defer(() -> {
            if (!allowed || session == null || !session.isOpen()) {
                if (throughputRecorder != null) throughputRecorder.recordSkipped();
                return Mono.just(false);
            }

            return Mono.fromCallable(() -> objectMapper.writeValueAsString(payload))
                .map(session::textMessage)
                .flatMap(msg -> session.send(Mono.just(msg)))
                // 전송 성공만 계측 — skip(null/closed 세션)·실패는 throughput에 포함하지 않는다
                .doOnSuccess(v -> {
                    if (throughputRecorder != null) {
                        throughputRecorder.recordSent();
                    }
                })
                .doOnCancel(() -> {
                    if (throughputRecorder != null) throughputRecorder.recordCancelled();
                })
                .thenReturn(true)
                // 전송 실패는 게임 진행을 막지 않는다 — 세션 사망은 disconnect 처리가 별도로 감지·수습
                .onErrorResume(e -> {
                    if (throughputRecorder != null) throughputRecorder.recordFailed();
                    log.debug("WS 메시지 전송 실패 — 세션 [{}] 스킵", session.getId(), e);
                    return Mono.just(false);
                });
        });
    }

    public <T> Mono<Void> sendMessageToAllUser(long roomId, WebSocketResDto<T> response) {
        // 수신자 조회는 반드시 구독 시점으로 지연 — assembly 시점에 평가하면
        // addPlayer(...).then(broadcast) 체인에서 세션 등록 전 수신자를 캡처해 첫 브로드캐스트가 유실된다
        return Flux.defer(() -> Flux.fromIterable(sessionManager.getAllUser(roomId)))
                .flatMap(session -> sendMessageToSession(session, response))
                .then();
    }
}
