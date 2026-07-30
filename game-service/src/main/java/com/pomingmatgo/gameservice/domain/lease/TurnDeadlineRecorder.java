package com.pomingmatgo.gameservice.domain.lease;

import com.pomingmatgo.gameservice.domain.repository.RoomLeaseRepository;
import com.pomingmatgo.gameservice.global.config.RoomLeaseProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

// 턴 deadline을 lease에 기록 — 인수 노드의 타이머 복원용. 방마다 최신값만 의미 있으므로
// conflate(map 덮어쓰기) 후 주기 flush 1왕복 — 커맨드마다 단건 UPDATE하면 스냅샷 단건 insert처럼 커밋 지배 항이 된다
@Component
@RequiredArgsConstructor
@Slf4j
public class TurnDeadlineRecorder {

    private final RoomLeaseRepository leaseRepository;
    private final RoomLeaseManager leaseManager;
    private final RoomLeaseProperties properties;
    private final ConcurrentHashMap<Long, Long> pending = new ConcurrentHashMap<>();
    private Disposable flushLoop;

    @PostConstruct
    void startFlushLoop() {
        if (!leaseRepository.enabled()) {
            return;
        }
        flushLoop = Flux.interval(properties.deadlineFlushInterval())
                .onBackpressureDrop()
                .concatMap(tick -> flush()
                        // 기록 실패 = 인수 시 즉시 자동플레이로 퇴화할 뿐 — correctness 무관이라 재시도 없이 버린다
                        .onErrorResume(e -> {
                            log.warn("턴 deadline flush 실패", e);
                            return Mono.empty();
                        }))
                .subscribe();
    }

    @PreDestroy
    void stopFlushLoop() {
        if (flushLoop != null) {
            flushLoop.dispose();
        }
    }

    /** 타이머 등록 지점에서 호출 — 비활성이면 무비용. deadline은 monotonic nanos로 받아 여기서만 wall clock으로 환산한다 */
    public void record(long roomId, long deadlineNanos) {
        if (!leaseRepository.enabled()) {
            return;
        }
        // wall clock 사용의 정당한 예외 — nanoTime 기준점은 프로세스 내부에서만 유효하므로
        // 크로스 프로세스 복원값만 epoch millis로 저장한다. 프로세스 내부 타이머 판정은 그대로 nanoTime
        long deadlineEpochMillis = System.currentTimeMillis()
                + TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        pending.put(roomId, deadlineEpochMillis);
    }

    private Mono<Void> flush() {
        if (pending.isEmpty()) {
            return Mono.empty();
        }
        List<RoomLeaseRepository.RoomDeadline> batch = new ArrayList<>();
        for (Map.Entry<Long, Long> entry : pending.entrySet()) {
            // 값 일치 remove — 순회 중 더 새 값이 들어왔다면 남겨서 다음 주기에 싣는다 (최신값 유실 방지)
            pending.remove(entry.getKey(), entry.getValue());
            Long token = leaseManager.tokenOf(entry.getKey());
            if (token != null) {
                batch.add(new RoomLeaseRepository.RoomDeadline(entry.getKey(), token, entry.getValue()));
            }
        }
        return leaseRepository.recordDeadlines(batch);
    }
}
