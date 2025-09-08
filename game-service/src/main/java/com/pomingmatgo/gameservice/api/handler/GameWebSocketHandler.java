package com.pomingmatgo.gameservice.api.handler;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.domain.service.matgo.PrePlayService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

@Component
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PrePlayService prePlayService = new PrePlayService();
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> handleMessage(message, session))
                .onErrorResume(e -> {
                    return Mono.empty(); //todo: error handling
                })
                .then();
    }

    private Mono<Void> handleMessage(WebSocketMessage message, WebSocketSession session) {
        try {
            RequestEvent event = objectMapper.readValue(message.getPayloadAsText(), RequestEvent.class);
            return handleEvent(event, session);
        } catch (JsonProcessingException e) {
            return Mono.empty();
        }
    }

    private Mono<Void> handleEvent(RequestEvent event, WebSocketSession session) {
        switch (/*todo: 작성 필요*/"AAA") {
            case "SET_LEAD_PLAYER":
                prePlayService.setLeadPlayer();
                return Mono.empty();
            default:
                return Mono.empty();
        }
    }
}
