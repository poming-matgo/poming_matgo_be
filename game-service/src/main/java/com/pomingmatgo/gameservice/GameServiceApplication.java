package com.pomingmatgo.gameservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Hooks;

@SpringBootApplication
public class GameServiceApplication {

    public static void main(String[] args) {
        Hooks.onOperatorDebug();
        SpringApplication.run(GameServiceApplication.class, args);
    }

}
