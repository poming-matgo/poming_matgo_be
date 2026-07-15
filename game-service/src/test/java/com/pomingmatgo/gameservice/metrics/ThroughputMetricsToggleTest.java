package com.pomingmatgo.gameservice.metrics;

import com.pomingmatgo.gameservice.global.MessageSender;
import com.pomingmatgo.gameservice.global.metrics.ThroughputRecorder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.web.reactive.function.server.RouterFunction;

import static org.assertj.core.api.Assertions.assertThat;

// metrics.throughput.enabled=false면 계측 recorder/엔드포인트 bean이 등록되지 않고,
// hot path(MessageSender)는 recorder 없이도 정상 기동해야 한다
@SpringBootTest(properties = {
        "metrics.throughput.enabled=false",
        "spring.autoconfigure.exclude="
                + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration"
})
class ThroughputMetricsToggleTest {

    @Autowired
    ApplicationContext context;

    @Test
    void disabled_registersNoMetricsBeans_butMessageSenderStillWorks() {
        assertThat(context.getBeanNamesForType(ThroughputRecorder.class)).isEmpty();
        assertThat(context.containsBean("routeThroughputMetrics")).isFalse();
        assertThat(context.getBeanNamesForType(RouterFunction.class))
                .noneMatch(name -> name.equals("routeThroughputMetrics"));
        assertThat(context.getBean(MessageSender.class)).isNotNull();
    }
}
