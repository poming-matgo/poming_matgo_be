package com.pomingmatgo.gameservice.domain.recovery;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandLog;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import com.pomingmatgo.gameservice.domain.lease.RoomLeaseManager;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameLogRepository;
import com.pomingmatgo.gameservice.domain.repository.GameSnapshotRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.RoomLeaseRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.GamePlayService;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshotService;
import com.pomingmatgo.gameservice.global.config.GameRecoveryProperties;
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import com.pomingmatgo.gameservice.scheduler.TurnScheduler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 2-D 복구 절차: 만료 lease 스캔 → 인수(fencing token 증가) → 스냅샷 restore + tail replay → 타이머 재등록.
 * 클라이언트 상태 동기화는 별도 프로토콜 없이 재접속 CONNECT의 RECONNECT_STATE 경로가 그대로 처리한다
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class GameRecoveryService {

    // replay는 방당 수십 커맨드의 인메모리 재실행이라 수 초면 끝난다 — TTL은 그 상한을 넉넉히 덮는 안전망
    private static final Duration REPLAY_GUARD_TTL = Duration.ofSeconds(30);
    private static final int SCAN_CONCURRENCY = 4;

    private final RoomLeaseManager leaseManager;
    private final GameLogRepository gameLogRepository;
    private final GameSnapshotRepository snapshotRepository;
    private final GameSnapshotService gameSnapshotService;
    private final GameStateRepository gameStateRepository;
    private final InstalledCardRepository installedCardRepository;
    private final AcquiredCardRepository acquiredCardRepository;
    private final PreGameService preGameService;
    private final GamePlayService gamePlayService;
    private final GameCommandLog gameCommandLog;
    private final InFlightManager inFlightManager;
    private final TurnScheduler turnScheduler;
    private final RoomCleanupService roomCleanupService;
    private final GameRecoveryProperties properties;
    private Disposable scanLoop;

    @PostConstruct
    void startScanLoop() {
        if (!enabled() || properties.scanInterval().isZero() || properties.scanInterval().isNegative()) {
            return;
        }
        scanLoop = Flux.interval(properties.scanInterval())
                // 이전 스캔이 밀리면 tick을 버린다 — 스캔 중복 실행 방지 (인수 자체는 DB UPDATE가 원자적 승자를 정한다)
                .onBackpressureDrop()
                .concatMap(tick -> scanOnce()
                        .onErrorResume(e -> {
                            log.error("복구 스캔 실패", e);
                            return Mono.empty();
                        }))
                .subscribe();
        log.info("만료 lease 인수 스캔 활성 — interval={}", properties.scanInterval());
    }

    @PreDestroy
    void stopScanLoop() {
        if (scanLoop != null) {
            scanLoop.dispose();
        }
    }

    /** 복구는 durable 로그(복원할 기록)와 lease(배타성) 둘 다 있어야 성립한다 */
    public boolean enabled() {
        return leaseManager.fencingEnabled() && gameLogRepository.enabled();
    }

    public Mono<Void> scanOnce() {
        return leaseManager.findExpiredRooms()
                .flatMap(roomId -> recoverRoom(roomId)
                        .onErrorResume(e -> {
                            log.error("방 복구 실패 — roomId={}", roomId, e);
                            return abortRecovery(roomId);
                        }), SCAN_CONCURRENCY)
                .then();
    }

    private Mono<Void> recoverRoom(long roomId) {
        // 로컬 상태가 있으면 이 인스턴스가 아직 살아있다고 믿는 방 — 스스로 인수하면 진행 중인 게임과 경합한다.
        // 진짜 좀비라면 다음 쓰기의 fencing 거부(LeaseLostEvent)가 정리하고, 그 뒤 스캔이 깨끗하게 인수한다
        return gameStateRepository.findById(roomId)
                .hasElement()
                .flatMap(locallyAlive -> locallyAlive
                        ? Mono.empty()
                        // 인수 UPDATE가 곧 상호 배제 — 동시 스캔(다른 노드 포함) 중 승자 1명만 진행한다
                        : leaseManager.takeover(roomId)
                                .flatMap(takeover -> dispatchRecovered(roomId, takeover)));
    }

    private Mono<Void> dispatchRecovered(long roomId, RoomLeaseRepository.Takeover takeover) {
        return gameLogRepository.latestGenerationCompleted(roomId)
                // 세대 없음 = DECK_INIT 전 크래시 — 복원할 기록이 없으니 완주와 동일하게 lease만 마감한다
                .defaultIfEmpty(true)
                .flatMap(completed -> {
                    if (completed) {
                        return leaseManager.release(roomId);
                    }
                    log.info("방 인수 — roomId={}, token={}", roomId, takeover.fencingToken());
                    return recover(roomId, takeover);
                });
    }

    private Mono<Void> recover(long roomId, RoomLeaseRepository.Takeover takeover) {
        // 복구 중 클라이언트 액션 차단 — 양 플레이어의 NORMAL InFlight를 선점해 기존 fail-fast(TRY_AGAIN) 경로로 밀어낸다.
        // GameState가 replay 중간 상태로 노출되는 창에 낡은 액션이 실행되는 것을 막는다 (자동플레이 타이머는 아직 없음)
        String guardToken = Long.toHexString(ThreadLocalRandom.current().nextLong());
        return Mono.usingWhen(
                acquireReplayGuard(roomId, guardToken),
                guarded -> loadDurable(roomId)
                        .flatMap(durable -> {
                            if (durable.isEmpty()) {
                                // 세대는 있는데 레코드가 전무 — DECK_INIT이 유실 창에 든 게임. 복원 불가로 마감한다
                                return gameLogRepository.markCompleted(roomId).then(leaseManager.release(roomId));
                            }
                            // 억제는 실제 replay가 확정된 뒤에만 시작 — 마감 경로에서 걸어두면 해제 없이 영구 잔류한다
                            gameCommandLog.beginRecovery(roomId);
                            return restoreState(roomId, durable)
                                    .then(Mono.defer(() -> finishRecovery(roomId, durable, takeover)));
                        }),
                guarded -> releaseReplayGuard(roomId, guardToken));
    }

    private Mono<Long> acquireReplayGuard(long roomId, String guardToken) {
        return Flux.just(Player.PLAYER_1, Player.PLAYER_2)
                .concatMap(player -> inFlightManager
                        .trySetFlag(InFlightManager.normalKey(roomId, player.getNumber()), guardToken, REPLAY_GUARD_TTL))
                .then(Mono.just(roomId));
    }

    private Mono<Void> releaseReplayGuard(long roomId, String guardToken) {
        return Flux.just(Player.PLAYER_1, Player.PLAYER_2)
                .concatMap(player -> inFlightManager
                        .deleteFlag(InFlightManager.normalKey(roomId, player.getNumber()), guardToken))
                .then();
    }

    private record Durable(GameSnapshot snapshot, List<GameLogRecord> tail, long lastSeq) {
        private boolean isEmpty() {
            return snapshot == null && tail.isEmpty();
        }
    }

    private Mono<Durable> loadDurable(long roomId) {
        return snapshotRepository.findLatest(roomId)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .flatMap(snapshot -> {
                    long fromSeq = snapshot.map(s -> s.seq() + 1).orElse(1L);
                    return gameLogRepository.findAllFromSeq(roomId, fromSeq)
                            .collectList()
                            .map(tail -> new Durable(snapshot.orElse(null), tail,
                                    tail.isEmpty()
                                            ? snapshot.map(GameSnapshot::seq).orElse(0L)
                                            : tail.get(tail.size() - 1).seq()));
                });
    }

    private Mono<Void> restoreState(long roomId, Durable durable) {
        if (durable.snapshot() != null) {
            return gameSnapshotService.restore(durable.snapshot())
                    .then(replay(roomId, durable.tail()));
        }
        // 스냅샷 없음 = 라운드 1 크래시 — DECK_INIT(세대 출생 기록)만으로 초기 상태를 복원한다
        GameLogRecord deckInit = durable.tail().get(0);
        if (deckInit.type() != GameCommandType.DECK_INIT) {
            return Mono.error(new IllegalStateException(
                    "스냅샷 없는 복구의 첫 레코드가 DECK_INIT이 아니다 — roomId=" + roomId + ", seq=" + deckInit.seq()));
        }
        return gameStateRepository.create(initialState(roomId, deckInit))
                .then(preGameService.distributeCards(roomId, deckInit.deck()))
                .then(replay(roomId, durable.tail().subList(1, durable.tail().size())));
    }

    private GameState initialState(long roomId, GameLogRecord deckInit) {
        return GameState.builder()
                .roomId(roomId)
                .player1(PlayerState.builder().userId(deckInit.user1Id()).ready(true).build())
                .player2(PlayerState.builder().userId(deckInit.user2Id()).ready(true).build())
                .leadingPlayer(deckInit.leadingPlayer())
                .currentTurn(1)
                .round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
    }

    private Mono<Void> replay(long roomId, List<GameLogRecord> tail) {
        return Flux.fromIterable(tail)
                .concatMap(record -> execute(roomId, record))
                .then();
    }

    private Mono<?> execute(long roomId, GameLogRecord record) {
        return switch (record.type()) {
            case NORMAL_SUBMIT -> gamePlayService.executeNormalSubmit(roomId, record.player(), record.cardIndex(), null);
            case FLOOR_SELECT -> gamePlayService.executeFloorSelection(roomId, record.player(), record.cardIndex(), null);
            case GO_STOP -> gamePlayService.executeGoStop(roomId, record.player(), record.go(), null);
            case DECK_INIT -> Mono.error(new IllegalStateException(
                    "replay tail 중간에 DECK_INIT — roomId=" + roomId + ", seq=" + record.seq()));
        };
    }

    private Mono<Void> finishRecovery(long roomId, Durable durable, RoomLeaseRepository.Takeover takeover) {
        return gameStateRepository.findById(roomId)
                .switchIfEmpty(Mono.error(new IllegalStateException("복구 직후 상태 유실 — roomId=" + roomId)))
                .flatMap(state -> {
                    // 이후 라이브 커맨드가 같은 세대의 seq 사슬을 잇도록 마지막 영속 seq에서 재개
                    gameCommandLog.endRecovery(roomId, durable.lastSeq());
                    if (state.getPhase() == GamePhase.END) {
                        // 마지막 커맨드가 게임을 끝냈는데 cleanup 전에 죽은 경우 — 완료 표시·lease 해제만 마저 한다
                        return roomCleanupService.cleanupRoomData(roomId);
                    }
                    if (state.getPhase().isPlayerActionPhase()) {
                        return Mono.fromRunnable(() -> scheduleTimer(state, takeover));
                    }
                    return Mono.error(new IllegalStateException(
                            "복구 후 예상 밖 phase — roomId=" + roomId + ", phase=" + state.getPhase()));
                });
    }

    private void scheduleTimer(GameState state, RoomLeaseRepository.Takeover takeover) {
        Player actor = state.getPhase() == GamePhase.AWAITING_FLOOR_CARD_CHOICE
                ? state.getChoiceInfo().getPlayerNumToChoose()
                : state.getCurrentPlayer();
        turnScheduler.scheduleAutoPlay(state.getRoomId(), state.getRound(), state.getCurrentTurn(), actor,
                restoredDeadlineNanos(takeover.turnDeadlineEpochMillis()), state.getPhase());
    }

    // wall clock 사용의 정당한 예외 — 크로스 프로세스 복원값(epoch)을 프로세스 로컬 nanos로 환산한다.
    // 이미 지난(또는 미기록) deadline은 즉시 발사(MIN_DELAY)로 수렴 — 기존 scheduleAutoPlay가 처리한다
    private long restoredDeadlineNanos(Long deadlineEpochMillis) {
        if (deadlineEpochMillis == null) {
            return System.nanoTime();
        }
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deadlineEpochMillis - System.currentTimeMillis());
    }

    // 실패 시 로컬 부분 상태를 지우고 lease를 즉시 만료로 되돌린다 — 다음 스캔(이 노드든 타 노드든)이 재시도한다
    private Mono<Void> abortRecovery(long roomId) {
        gameCommandLog.abortRecovery(roomId);
        return Mono.when(
                        gameStateRepository.cleanup(roomId),
                        installedCardRepository.cleanup(roomId),
                        acquiredCardRepository.cleanup(roomId))
                .then(leaseManager.abandon(roomId));
    }
}
