package com.pomingmatgo.gameservice.domain.snapshot;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameSnapshotRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
@Slf4j
public class GameSnapshotService {

    private final GameStateRepository gameStateRepository;
    private final InstalledCardRepository installedCardRepository;
    private final AcquiredCardRepository acquiredCardRepository;
    private final GameSnapshotRepository snapshotRepository;

    /**
     * 라운드 경계에서 seq 시점 스냅샷을 캡처한다 — @GameLock 안에서 호출해야 4개 저장소가 같은 seq 시점이다(torn 방지).
     * 값 캡처만 락 안에서 하고 직렬화·저장은 락 밖 비동기 — 실패는 복구 replay가 길어질 뿐이라 게임 경로를 막지 않는다
     */
    public Mono<Void> captureIfRoundStart(long roomId, long seq, GamePhase persistedPhase, GameState nextState) {
        if (!snapshotRepository.enabled() || !isRoundStart(persistedPhase, nextState)) {
            return Mono.empty();
        }
        return Mono.zip(
                        installedCardRepository.getPlayerCards(roomId, Player.PLAYER_1),
                        installedCardRepository.getPlayerCards(roomId, Player.PLAYER_2),
                        installedCardRepository.getAllRevealedCards(roomId),
                        installedCardRepository.getHiddenCards(roomId),
                        acquiredCardRepository.getAllCards(roomId, 1),
                        acquiredCardRepository.getAllCards(roomId, 2))
                .map(t -> new GameSnapshot(roomId, seq, nextState,
                        t.getT1(), t.getT2(), t.getT3(), t.getT4(), t.getT5(), t.getT6()))
                .doOnNext(snapshot -> snapshotRepository.save(snapshot)
                        .subscribeOn(Schedulers.boundedElastic())
                        .subscribe(unused -> { },
                                e -> log.error("스냅샷 저장 실패 — roomId={}, seq={}", roomId, seq, e)))
                .then();
    }

    // 게임 중 커맨드가 IN_PROGRESS·currentTurn 1로 끝나는 지점은 setNextTurn이 round를 올린 직후뿐이다.
    // phase는 nextState가 아니라 저장된 값(persistedPhase)으로 판정한다 — 선택 대기로 끝난 턴은 nextState가 stale이다
    private boolean isRoundStart(GamePhase persistedPhase, GameState nextState) {
        return persistedPhase == GamePhase.IN_PROGRESS && nextState.getCurrentTurn() == 1;
    }

    /** 복구 = restore + seq 이후 로그 replay (추후 구현). 대상 방의 저장소들은 비어 있어야 한다 */
    public Mono<Void> restore(GameSnapshot snapshot) {
        long roomId = snapshot.roomId();
        return gameStateRepository.create(snapshot.gameState())
                .then(Mono.zip(
                        installedCardRepository.savePlayerCards(snapshot.p1Hand(), roomId, Player.PLAYER_1),
                        installedCardRepository.savePlayerCards(snapshot.p2Hand(), roomId, Player.PLAYER_2),
                        installedCardRepository.saveRevealedCard(snapshot.floorCards(), roomId),
                        installedCardRepository.saveHiddenCard(snapshot.hiddenDeck(), roomId)))
                .then(acquiredCardRepository.addCards(roomId, 1, snapshot.p1Acquired()))
                .then(acquiredCardRepository.addCards(roomId, 2, snapshot.p2Acquired()))
                .then();
    }
}
