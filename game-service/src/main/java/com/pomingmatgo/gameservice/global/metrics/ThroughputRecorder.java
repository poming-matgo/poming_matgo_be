package com.pomingmatgo.gameservice.global.metrics;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * 서버 측 초당 WS 송신 메시지 계측.
 * k6(클라이언트) 집계는 대규모 부하에서 시계열 sink의 backpressure 한계(InfluxDB 적재 유실)가 있어,
 * 1초 단위 throughput 시계열은 서버가 직접 센다 — README 부하 테스트 방법론 참조.
 * hot path 비용은 LongAdder.increment() 1회. 방 단위 상태가 아니므로 RoomCleanupService 대상 아님.
 */
@Component
public class ThroughputRecorder {
    private static final int MAX_SAMPLES = 2 * 60 * 60; // 초 단위 샘플 최대 2시간 보관

    private final LongAdder counter = new LongAdder();
    private final ArrayDeque<Long> samples = new ArrayDeque<>();
    private long totalSent;
    private ScheduledExecutorService sampler;

    public void recordSent() {
        counter.increment();
    }

    @PostConstruct
    void startSampling() {
        sampler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ws-throughput-sampler");
            t.setDaemon(true);
            return t;
        });
        sampler.scheduleAtFixedRate(this::sample, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    void stopSampling() {
        sampler.shutdownNow();
    }

    private synchronized void sample() {
        long sent = counter.sumThenReset();
        totalSent += sent;
        if (samples.size() >= MAX_SAMPLES) {
            samples.pollFirst();
        }
        samples.addLast(sent);
    }

    public synchronized Snapshot snapshot() {
        List<Long> copy = new ArrayList<>(samples);
        long max = copy.stream().mapToLong(Long::longValue).max().orElse(0);
        return new Snapshot(totalSent + counter.sum(), copy.size(), max, copy);
    }

    public synchronized void reset() {
        counter.reset();
        totalSent = 0;
        samples.clear();
    }

    public record Snapshot(long totalSent, int seconds, long maxPerSec, List<Long> perSecond) {}
}
