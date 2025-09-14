package com.pomingmatgo.gameservice.api.handler;

import com.pomingmatgo.gameservice.api.request.CreateRoomRequest;
import com.pomingmatgo.gameservice.api.request.DeleteRoomRequest;
import com.pomingmatgo.gameservice.api.request.JoinRoomRequest;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomService;
import com.pomingmatgo.gameservice.global.ApiResponseDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
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
                        .bodyValue(new ApiResponseDto<>(HttpStatus.CREATED.value(), String.format("%d번 게임방이 생성됐습니다.", roomId))));
    }

    public Mono<ServerResponse> joinRoom(ServerRequest request) {

        return request.bodyToMono(JoinRoomRequest.class)
                .flatMap(req -> roomService.joinRoom(req.getUserId(), req.getRoomId())
                        .thenReturn(req))
                .flatMap(req -> ServerResponse.ok().bodyValue(
                        new ApiResponseDto<>(
                                HttpStatus.OK.value(),
                                String.format("%d번 게임방에 %d번 플레이어가 조인했습니다.", req.getRoomId(), req.getUserId())
                        )
                ));
    }

    public Mono<ServerResponse> deleteRoom(ServerRequest request) {
        return request.bodyToMono(DeleteRoomRequest.class)
                .flatMap(req -> roomService.deleteRoom(req.getRoomId())
                        .thenReturn(req))
                .flatMap(req -> ServerResponse.ok().bodyValue(
                        new ApiResponseDto<>(
                                HttpStatus.OK.value(),
                                String.format("%d번 게임방이 삭제됐습니다.", req.getRoomId())
                        )
                ));
    }
}
