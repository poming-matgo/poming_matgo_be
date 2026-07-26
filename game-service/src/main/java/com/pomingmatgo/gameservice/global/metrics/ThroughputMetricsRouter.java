package com.pomingmatgo.gameservice.global.metrics;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;

// 부하 테스트용 내부 엔드포인트 — DELETE는 run 사이 초기화용. Recorder가 꺼져 있으면 등록되지 않는다
@Configuration
public class ThroughputMetricsRouter {
    @Bean
    @ConditionalOnProperty(name = "metrics.throughput.enabled", havingValue = "true", matchIfMissing = true)
    public RouterFunction<ServerResponse> routeThroughputMetrics(ThroughputRecorder recorder) {
        return route()
                .GET("/internal/metrics/throughput", req -> ServerResponse.ok().bodyValue(recorder.snapshot()))
                .DELETE("/internal/metrics/throughput", req -> {
                    recorder.reset();
                    return ServerResponse.noContent().build();
                })
                .build();
    }
}
