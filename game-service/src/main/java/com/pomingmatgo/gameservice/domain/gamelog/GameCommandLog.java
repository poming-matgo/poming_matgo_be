package com.pomingmatgo.gameservice.domain.gamelog;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.GameLogRepository;
import com.pomingmatgo.gameservice.global.config.GameLogBatchProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongFunction;

// seq 부여·enqueue는 @GameLock 안(호출자 책임), 실제 append는 방 단위 writer가 락 밖에서 직렬 수행한다.
// seq 카운터와 Sink는 인스턴스 로컬 — AutoPlayScheduler와 같은 sticky routing 전제
@Component
@RequiredArgsConstructor
@Slf4j
public class GameCommandLog {
    private final GameLogRepository gameLogRepository;
    // 배치 크기/주기가 곧 유실 창 — durability 곡선의 측정 변수 (game.log.batch.*)
    private final GameLogBatchProperties batchProperties;
    private final ConcurrentHashMap<Long, RoomLogChannel> channels = new ConcurrentHashMap<>();

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
            Sinks.EmitResult result = sink.tryEmitNext(record);
            if (result.isFailure()) {
                log.warn("게임 로그 emit 실패({}) — roomId={}, seq={}", result, record.roomId(), record.seq());
            } else {
                // RPO 실측용 ground truth — kill -9 후 이 로그와 저장소 diff로 유실 커맨드를 센다 (평소 TRACE라 무비용)
                log.trace("emit roomId={} seq={}", record.roomId(), record.seq());
            }
            return record.seq();
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
        return Mono.fromCallable(() ->
                channels.computeIfAbsent(roomId, RoomLogChannel::new).emit(recordFactory));
    }

    /** 잔여 배치 drain 완료까지 대기 후 완료 표시 — cleanup ≠ delete, 레코드는 저장소에 남는다 */
    public Mono<Void> close(long roomId) {
        return Mono.defer(() -> {
            RoomLogChannel channel = channels.remove(roomId);
            if (channel == null) {
                return Mono.empty();
            }
            channel.sink.tryEmitComplete();
            return channel.drained.asMono();
        });
    }
}
