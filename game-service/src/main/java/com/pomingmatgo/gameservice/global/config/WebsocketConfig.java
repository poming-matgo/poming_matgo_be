package com.pomingmatgo.gameservice.global.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import reactor.core.publisher.Sinks;

import java.util.Map;

@Configuration
public class WebsocketConfig {
    @Bean
    public SimpleUrlHandlerMapping handlerMapping(WebSocketHandler wsh) {
        return new SimpleUrlHandlerMapping(Map.of("/gostop", wsh), 1);
    }

    @Bean
    public WebSocketHandlerAdapter webSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }

    @Bean
    public Sinks.One<String> sink() {
        return Sinks.one();
    }
}
