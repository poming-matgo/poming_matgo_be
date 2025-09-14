package com.pomingmatgo.gameservice.api.handler;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.domain.service.matgo.PrePlayService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final PrePlayService prePlayService;
    private final RoomService roomService;
    private final SessionManager sessionManager = new SessionManager();
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> handleMessage(message, session))
                .then();
    }

    private Mono<Void> handleMessage(WebSocketMessage message, WebSocketSession session) {
        return Mono.fromCallable(() -> objectMapper.readValue(message.getPayloadAsText(), RequestEvent.class))
                .flatMap(event -> routeEvent(event, session))
                .onErrorResume(JsonProcessingException.class, e -> Mono.empty());
    }

    private Mono<Void> routeEvent(RequestEvent event, WebSocketSession session) {
        Long userId = event.getUserId();
        sessionManager.addSessionIfAbsent(userId.toString(), session);
        String eventType = event.getEventType().getSubType();
        switch(eventType) {
            case "READY":
                return roomService.ready(event.getUserId(), event.getRoomId());
        }
        return Mono.empty();
    }
}
