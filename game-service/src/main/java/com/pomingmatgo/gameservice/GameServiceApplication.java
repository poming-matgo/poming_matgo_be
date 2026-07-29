package com.pomingmatgo.gameservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Hooks;

// R2dbcAutoConfiguration 제외 — r2dbc-spi가 클래스패스에 있으면 spring.r2dbc.url 없이는 기동이 실패한다.
// 게임 로그 접속은 GameLogPostgresConfig가 game.log.postgres.* 로 직접 구성한다 (직교 축이라 프로파일·spring.r2dbc와 무관)
@SpringBootApplication(exclude = R2dbcAutoConfiguration.class)
public class GameServiceApplication {

    public static void main(String[] args) {
        /*Hooks.onOperatorDebug();
        BlockHound.builder()
                .allowBlockingCallsInside("org.redisson.command.CommandAsyncService", "get")
                .allowBlockingCallsInside("org.redisson.connection.pool.ConnectionPool", "get")
                .allowBlockingCallsInside("org.redisson.PubSubMessageListener", "onMessage")

                .allowBlockingCallsInside("java.util.concurrent.ThreadPoolExecutor", "getTask")
                .allowBlockingCallsInside("java.util.concurrent.LinkedBlockingQueue", "take")

                .install();*/
        SpringApplication.run(GameServiceApplication.class, args);
    }

}
