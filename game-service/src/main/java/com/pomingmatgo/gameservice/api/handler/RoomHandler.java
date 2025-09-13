package com.pomingmatgo.gameservice.api.handler;

import com.pomingmatgo.gameservice.api.request.CreateRoomRequest;
import com.pomingmatgo.gameservice.api.request.JoinRoomRequest;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.net.URI;

@Component
@RequiredArgsConstructor
public class RoomHandler {

    private final RoomService roomService;
    public Mono<ServerResponse> createRoom(ServerRequest request) {
        return request.bodyToMono(CreateRoomRequest.class)
                .flatMap(req -> roomService.createRoom(req.getRoomId()))
                .flatMap(roomId -> ServerResponse
                        .created(URI.create(String.format("/room/%d", roomId)))
                        .build());
    }

    public Mono<ServerResponse> joinRoom(ServerRequest request) {
        return request.bodyToMono(JoinRoomRequest.class)
                .flatMap(req -> roomService.joinRoom(req.getUserId(), req.getRoomId()))
                .flatMap(roomId -> ServerResponse
                        .ok()
                        .build());
    }
}
