package com.pomingmatgo.gameservice.global.session;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Sinks;

@Setter
@Getter
@NoArgsConstructor
public class RoomSessionData {
    private WebSocketSession player1Session;
    private WebSocketSession player2Session;
}
