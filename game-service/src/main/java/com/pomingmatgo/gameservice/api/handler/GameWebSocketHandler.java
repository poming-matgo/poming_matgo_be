package com.pomingmatgo.gameservice.api.handler;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.domain.service.matgo.PrePlayService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final PrePlayService prePlayService;
    private final RoomService roomService;
    private final ConcurrentHashMap<String, Long> userRoomMap = new ConcurrentHashMap<>();
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> handleMessage(message, session))
                .onErrorResume(e -> Mono.empty()) //todo: error handling
                .then();
    }

    private Mono<Long> handleMessage(WebSocketMessage message, WebSocketSession session) {
        return Mono.fromCallable(() -> objectMapper.readValue(message.getPayloadAsText(), RequestEvent.class))
                .flatMap(event -> handleEvent(event, session))
                .onErrorResume(JsonProcessingException.class, e -> Mono.empty());
    }

    private Mono<Long> handleEvent(RequestEvent event, WebSocketSession session) {
        switch (event.getEventType().getSubType()) {
            case "JOIN_ROOM":
                return roomService.joinRoom(event.getUserId())
                        .flatMap(roomId -> {
                            session.getAttributes().put("roomId", roomId);
                            userRoomMap.put(session.getId(), roomId);
                            return Mono.just(roomId);
                        });
            case "CONNECT_USER":
                prePlayService.connectUser(event.getUserId());
                return Mono.empty();
            case "SET_LEAD_PLAYER":
                prePlayService.setLeadPlayer();
                return Mono.empty();
            default:
                return Mono.empty();
        }
    }
}
