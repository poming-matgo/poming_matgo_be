package com.pomingmatgo.gameservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Hooks;

@SpringBootApplication
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
