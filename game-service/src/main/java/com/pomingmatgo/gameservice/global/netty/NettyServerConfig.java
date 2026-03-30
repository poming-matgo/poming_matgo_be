package com.pomingmatgo.gameservice.global.netty;

import org.springframework.boot.web.embedded.netty.NettyServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NettyServerConfig {

    @Bean
    public NettyServerCustomizer nettyServerCustomizer() {
        return httpServer -> httpServer.doOnConnection(connection -> {
            connection.addHandlerFirst("arrivalTimeRecorder", new ArrivalTimeRecordHandler());
        });
    }
}