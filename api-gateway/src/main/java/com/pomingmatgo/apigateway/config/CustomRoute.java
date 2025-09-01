package com.pomingmatgo.apigateway.config;

import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.cloud.gateway.route.RouteLocator;

@Configuration
public class CustomRoute {
    @Bean
    public RouteLocator routing(RouteLocatorBuilder builder) {

        return builder.routes()
                .route("user", r -> r.path("/user/**")
                        .uri("http://localhost:8082"))
                .build();
    }
}
