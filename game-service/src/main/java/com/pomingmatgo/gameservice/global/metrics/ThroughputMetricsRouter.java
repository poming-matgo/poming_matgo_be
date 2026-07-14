package com.pomingmatgo.gameservice.global.metrics;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;

/**
 * 부하 테스트용 내부 계측 조회 엔드포인트.
 * GET: 누적/초당 송신 메시지 스냅샷, DELETE: 측정 리셋 (run 사이 초기화용)
 */
@Configuration
public class ThroughputMetricsRouter {
    @Bean
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
