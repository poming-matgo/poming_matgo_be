package com.pomingmatgo.gameservice.api.request.websocket;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JoinRoomReq {
    private Long roomId;
    private Long userId;
}
