package com.pomingmatgo.gameservice.domain.gamelog;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameLogRepository;
import com.pomingmatgo.gameservice.global.config.GameLogBatchProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongFunction;

// seq 부여·enqueue는 @GameLock 안(호출자 책임), 실제 append는 ordered writer가 락 밖에서 직렬 수행한다.
// seq 카운터와 Sink는 인스턴스 로컬 — AutoPlayScheduler와 같은 sticky routing 전제
@Component
@Slf4j
public class GameCommandLog {
    private final GameLogRepository gameLogRepository;
    // 배치 크기/주기가 곧 유실 창 — durability 곡선의 측정 변수 (game.log.batch.*)
    private final GameLogBatchProperties batchProperties;
    private final ConcurrentHashMap<Long, RoomLogChannel> channels = new ConcurrentHashMap<>();
    // cross-room 모드: 전역 writer 1개가 여러 방을 한 배치(=insert 1왕복)로 묶는다 — 방 단위 배치는 cadence상 배치≈1
    private final CrossRoomChannel crossRoomChannel;

    public GameCommandLog(GameLogRepository gameLogRepository, GameLogBatchProperties batchProperties) {
        this.gameLogRepository = gameLogRepository;
        this.batchProperties = batchProperties;
        this.crossRoomChannel = batchProperties.crossRoom() && gameLogRepository.enabled() ? new CrossRoomChannel() : null;
    }

    // 방마다 Sink 하나 + concatMap — 세션 backpressure(GameWebSocketHandler)와 같은 패턴으로 append 순서를 보장한다
    private final class RoomLogChannel {
        private final Sinks.Many<GameLogRecord> sink = Sinks.many().unicast().onBackpressureBuffer();
        private final Sinks.Empty<Void> drained = Sinks.empty();
        private long seq;

        private RoomLogChannel(long roomId) {
            sink.asFlux()
                    .bufferTimeout(batchProperties.maxSize(), batchProperties.flushInterval())
                    .onBackpressureBuffer()
                    .concatMap(batch -> gameLogRepository.append(roomId, batch))
                    .then(Mono.defer(() -> gameLogRepository.markCompleted(roomId)))
                    // 로그 실패가 방 teardown을 막으면 안 된다
                    .onErrorResume(e -> {
                        log.error("게임 로그 writer 실패 — roomId={}", roomId, e);
                        return Mono.empty();
                    })
                    .doFinally(signal -> drained.tryEmitEmpty())
                    .subscribe();
        }

        // @GameLock이 방 단위 직렬화를 보장하므로 synchronized는 경합용이 아니라 스레드 간 seq 가시성용이다
        private synchronized long emit(LongFunction<GameLogRecord> recordFactory) {
            GameLogRecord record = recordFactory.apply(++seq);
            logEmitResult(sink.tryEmitNext(record), record);
            return record.seq();
        }
    }

    // 방 해시로 shard 고정 — 방 단위 순서는 shard 안의 단일 writer가, shard 간은 병렬로 insert 왕복을 나눈다.
    // 전역 채널 1개(락 1개 + 직렬 writer 1개)는 emit convoy·큐 적체로 스톨했다
    private final class CrossRoomChannel {
        private final Shard[] shards;
        private final ConcurrentHashMap<Long, RoomProgress> rooms = new ConcurrentHashMap<>();

        private CrossRoomChannel() {
            shards = new Shard[batchProperties.writerShards()];
            for (int i = 0; i < shards.length; i++) {
                shards[i] = new Shard();
            }
        }

        private long emit(long roomId, LongFunction<GameLogRecord> recordFactory) {
            RoomProgress progress = rooms.computeIfAbsent(roomId, k -> new RoomProgress());
            // 같은 방의 emit은 @GameLock이 직렬화 — seq 부여와 shard push 사이에 같은 방의 경쟁자는 없다
            GameLogRecord record = recordFactory.apply(progress.nextSeq());
            shards[(int) Math.floorMod(roomId, shards.length)].push(new EnqueuedRecord(record, progress));
            return record.seq();
        }

        private Mono<Void> close(long roomId) {
            // 즉시 제거 — 방 재사용 시 새 게임은 fresh seq(1부터)로 시작한다 (per-room 모드와 동일)
            RoomProgress progress = rooms.remove(roomId);
            if (progress == null) {
                return Mono.empty();
            }
            return progress.drained().then(gameLogRepository.markCompleted(roomId));
        }

        private final class Shard {
            private final Sinks.Many<EnqueuedRecord> sink = Sinks.many().unicast().onBackpressureBuffer();

            private Shard() {
                sink.asFlux()
                        .bufferTimeout(batchProperties.maxSize(), batchProperties.flushInterval())
                        .onBackpressureBuffer()
                        .concatMap(batch -> gameLogRepository
                                .appendAll(batch.stream().map(EnqueuedRecord::record).toList())
                                // 로그 실패가 게임 진행을 막으면 안 된다 — 실패 배치도 drain으로 간주 (1-C 계약)
                                .onErrorResume(e -> {
                                    log.error("cross-room 로그 배치 실패 — {}건", batch.size(), e);
                                    return Mono.empty();
                                })
                                .then(Mono.fromRunnable(() -> batch.forEach(EnqueuedRecord::markFlushed))))
                        .subscribe();
            }

            // unicast sink는 비직렬 emit을 거부한다 — shard 단위 synchronized가 유일한 emit 경합 지점
            private synchronized void push(EnqueuedRecord enqueued) {
                logEmitResult(sink.tryEmitNext(enqueued), enqueued.record());
            }
        }
    }

    // flushed 고수위는 EnqueuedRecord가 쥔 참조로 갱신 — close가 맵에서 제거한 뒤에도 대기자 신호가 도달한다
    private static final class RoomProgress {
        private long emitted;
        private long flushed;
        private Sinks.Empty<Void> waiter;
        private long target;

        private synchronized long nextSeq() {
            return ++emitted;
        }

        private synchronized void markFlushed(long seq) {
            flushed = seq;
            if (waiter != null && flushed >= target) {
                waiter.tryEmitEmpty();
            }
        }

        private synchronized Mono<Void> drained() {
            if (flushed >= emitted) {
                return Mono.empty();
            }
            target = emitted;
            waiter = Sinks.empty();
            return waiter.asMono();
        }
    }

    private record EnqueuedRecord(GameLogRecord record, RoomProgress progress) {
        private void markFlushed() {
            progress.markFlushed(record.seq());
        }
    }

    private void logEmitResult(Sinks.EmitResult result, GameLogRecord record) {
        if (result.isFailure()) {
            log.warn("게임 로그 emit 실패({}) — roomId={}, seq={}", result, record.roomId(), record.seq());
        } else {
            // RPO 실측용 ground truth — kill -9 후 이 로그와 저장소 diff로 유실 커맨드를 센다 (평소 TRACE라 무비용)
            log.trace("emit roomId={} seq={}", record.roomId(), record.seq());
        }
    }

    public Mono<Long> logDeckInit(long roomId, List<Card> deck) {
        return enqueue(roomId, seq -> GameLogRecord.deckInit(roomId, seq, deck));
    }

    /** 부여된 seq를 반환한다 — 스냅샷의 일관성 지점(seq N 시점)이 이 값으로 정의된다. 비활성이면 empty */
    public Mono<Long> logCommand(long roomId, GameCommandType type, Player player, int cardIndex,
                                 boolean go, GamePhase prevPhase, GamePhase nextPhase) {
        return enqueue(roomId, seq -> GameLogRecord.command(roomId, seq, type, player, cardIndex, go, prevPhase, nextPhase));
    }

    private Mono<Long> enqueue(long roomId, LongFunction<GameLogRecord> recordFactory) {
        if (!gameLogRepository.enabled()) {
            return Mono.empty();
        }
        if (crossRoomChannel != null) {
            return Mono.fromCallable(() -> crossRoomChannel.emit(roomId, recordFactory));
        }
        return Mono.fromCallable(() ->
                channels.computeIfAbsent(roomId, RoomLogChannel::new).emit(recordFactory));
    }

    /** 잔여 배치 drain 완료까지 대기 후 완료 표시 — cleanup ≠ delete, 레코드는 저장소에 남는다 */
    public Mono<Void> close(long roomId) {
        return Mono.defer(() -> {
            if (crossRoomChannel != null) {
                return crossRoomChannel.close(roomId);
            }
            RoomLogChannel channel = channels.remove(roomId);
            if (channel == null) {
                return Mono.empty();
            }
            channel.sink.tryEmitComplete();
            return channel.drained.asMono();
        });
    }
}
