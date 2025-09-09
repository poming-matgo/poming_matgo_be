package com.pomingmatgo.gameservice.api.handler;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.domain.service.matgo.PrePlayService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class GameWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PrePlayService prePlayService = new PrePlayService();
    private final RoomService roomService = new RoomService();
    private final ConcurrentHashMap<String, Long> userRoomMap = new ConcurrentHashMap<>();
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
            case "CREATE_ROOM":
                long roomId = roomService.createRoom();
                session.getAttributes().put("roomId", roomId);
                userRoomMap.put(session.getId(), roomId);
                return Mono.empty();
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
