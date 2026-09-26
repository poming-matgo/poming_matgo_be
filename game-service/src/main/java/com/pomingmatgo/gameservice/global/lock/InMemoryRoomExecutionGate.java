package com.pomingmatgo.gameservice.global.lock;

import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN;

@Component
@Profile("in-memory")
public class InMemoryRoomExecutionGate {
    // 방별 액션·정리·생성의 진입만 직렬화하며, 실행 구독과 호출자 취소는 소유하지 않는다.
    private final Map<Long, Entry> rooms = new HashMap<>();

    public synchronized Entry acquire(long roomId) {
        Entry entry = rooms.computeIfAbsent(roomId, ignored -> new Entry());
        if (entry.active || entry.cleanups > 0 || entry.failed) throw new WebSocketBusinessException(TRY_AGAIN);
        entry.active = true;
        entry.drained = Sinks.empty();
        return entry;
    }

    public synchronized void accept(Entry entry) {
        if (!entry.active || entry.cleanups > 0 || entry.failed) throw new WebSocketBusinessException(TRY_AGAIN);
    }

    public synchronized void fail(Entry entry) {
        entry.failed = true;
    }

    public void release(Entry entry) {
        Sinks.Empty<Void> drained;
        synchronized (this) {
            entry.active = false;
            drained = entry.drained;
        }
        // 완료 신호는 정리 체인을 실행할 수 있으므로 monitor 밖에서 전달한다.
        drained.tryEmitEmpty();
    }

    public synchronized <T> T create(long roomId, Supplier<T> creation) {
        Entry entry = rooms.get(roomId);
        if (entry != null && (entry.active || entry.cleanups > 0 || entry.failed)) {
            throw new BusinessException(ErrorCode.ALREADY_EXISTED_ROOM);
        }
        return creation.get();
    }

    public Mono<Void> withCleanup(long roomId, Supplier<Mono<Void>> cleanup) {
        return Mono.usingWhen(Mono.fromSupplier(() -> beginCleanup(roomId)),
                lease -> lease.drained().then(Mono.defer(cleanup)),
                lease -> finishCleanup(roomId, lease.entry(), true),
                (lease, error) -> finishCleanup(roomId, lease.entry(), false),
                lease -> finishCleanup(roomId, lease.entry(), false));
    }

    private synchronized CleanupLease beginCleanup(long roomId) {
        Entry entry = rooms.computeIfAbsent(roomId, ignored -> new Entry());
        entry.cleanups++;
        return new CleanupLease(entry, entry.active ? entry.drained.asMono() : Mono.empty());
    }

    private Mono<Void> finishCleanup(long roomId, Entry entry, boolean completed) {
        return Mono.fromRunnable(() -> {
            synchronized (this) {
                if (completed) entry.failed = false;
                if (--entry.cleanups == 0 && !entry.active && !entry.failed) rooms.remove(roomId, entry);
            }
        });
    }

    public synchronized void discardIdle(long roomId) {
        Entry entry = rooms.get(roomId);
        if (entry != null && !entry.active && entry.cleanups == 0 && !entry.failed) rooms.remove(roomId, entry);
    }

    public static final class Entry {
        private boolean active;
        private boolean failed;
        private int cleanups;
        private Sinks.Empty<Void> drained = Sinks.empty();
    }

    private record CleanupLease(Entry entry, Mono<Void> drained) {}
}
