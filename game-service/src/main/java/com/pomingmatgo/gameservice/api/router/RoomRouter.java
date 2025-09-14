package com.pomingmatgo.gameservice.api.router;

import com.pomingmatgo.gameservice.api.handler.RoomHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
public class RoomRouter {
    @Bean
    public RouterFunction<ServerResponse> routeRoom(RoomHandler handler) {
        return route()
                .POST("/room", handler::createRoom)
                .POST("/room/join", handler::joinRoom)
                .DELETE("/room", handler::deleteRoom)
                .build();
    }
}
