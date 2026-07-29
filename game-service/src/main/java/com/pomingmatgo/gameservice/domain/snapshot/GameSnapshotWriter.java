package com.pomingmatgo.gameservice.domain.snapshot;

import com.pomingmatgo.gameservice.domain.repository.GameSnapshotRepository;
import com.pomingmatgo.gameservice.global.config.GameLogBatchProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

// 스냅샷 단건 insert가 durable 커밋의 지배 항 — 로그와 같은 방 해시 shard 배치로 왕복을 줄인다 (opt-in).
// 유실 = replay 연장일 뿐이라 로그 writer와 달리 drain/close 계약이 없고, 방 단위 상태도 없다 (cleanup 등록 불필요)
@Component
@Slf4j
public class GameSnapshotWriter {

    private final GameSnapshotRepository snapshotRepository;
    private final GameLogBatchProperties batchProperties;
    private final Shard[] shards;

    public GameSnapshotWriter(GameSnapshotRepository snapshotRepository, GameLogBatchProperties batchProperties) {
        this.snapshotRepository = snapshotRepository;
        this.batchProperties = batchProperties;
        this.shards = batchProperties.snapshotCrossRoom() && snapshotRepository.enabled()
                ? createShards(batchProperties.writerShards()) : null;
    }

    private Shard[] createShards(int count) {
        Shard[] created = new Shard[count];
        for (int i = 0; i < created.length; i++) {
            created[i] = new Shard();
        }
        return created;
    }

    /** fire-and-forget — 게임 경로를 막지 않는다. 배치 off면 단건 저장 폴백 */
    public void submit(GameSnapshot snapshot) {
        if (shards == null) {
            snapshotRepository.save(snapshot)
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe(unused -> { },
                            e -> log.error("스냅샷 저장 실패 — roomId={}, seq={}", snapshot.roomId(), snapshot.seq(), e));
            return;
        }
        shards[(int) Math.floorMod(snapshot.roomId(), shards.length)].push(snapshot);
    }

    private final class Shard {
        private final Sinks.Many<GameSnapshot> sink = Sinks.many().unicast().onBackpressureBuffer();

        private Shard() {
            sink.asFlux()
                    .bufferTimeout(batchProperties.maxSize(), batchProperties.flushInterval())
                    .onBackpressureBuffer()
                    .concatMap(batch -> snapshotRepository.saveAll(batch)
                            // 스냅샷 실패가 파이프라인을 끊으면 안 된다 — 배치 단위로 버리고 계속
                            .onErrorResume(e -> {
                                log.error("스냅샷 배치 저장 실패 — {}건", batch.size(), e);
                                return Mono.empty();
                            }))
                    .subscribe();
        }

        // unicast sink는 비직렬 emit을 거부한다 — shard 단위 synchronized가 유일한 emit 경합 지점
        private synchronized void push(GameSnapshot snapshot) {
            Sinks.EmitResult result = sink.tryEmitNext(snapshot);
            if (result.isFailure()) {
                log.warn("스냅샷 emit 실패({}) — roomId={}, seq={}", result, snapshot.roomId(), snapshot.seq());
            }
        }
    }
}
