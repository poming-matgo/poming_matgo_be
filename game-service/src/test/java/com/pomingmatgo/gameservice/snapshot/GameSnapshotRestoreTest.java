package com.pomingmatgo.gameservice.snapshot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameLogRepository;
import com.pomingmatgo.gameservice.domain.repository.GameSnapshotRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InMemoryGameSnapshotRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.GamePlayService;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshot;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshotService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude="
                + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration",
        "game.log.store=in-memory"})
@DisplayName("스냅샷: 라운드 경계 스냅샷 + seq 이후 tail replay로 완주 게임이 재구성된다")
class GameSnapshotRestoreTest {

    @Autowired GamePlayService gamePlayService;
    @Autowired PreGameService preGameService;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired InstalledCardRepository installedCardRepository;
    @Autowired AcquiredCardRepository acquiredCardRepository;
    @Autowired RoomCleanupService roomCleanupService;
    @Autowired GameLogRepository gameLogRepository;
    @Autowired GameSnapshotRepository snapshotRepository;
    @Autowired GameSnapshotService gameSnapshotService;
    @Autowired ObjectMapper objectMapper;

    private static final long ROOM = 970_001L;
    private static final int MAX_COMMANDS = 80;
    private static final int MAX_ATTEMPTS = 5;

    private record RoomFinalState(String gameState, List<Card> p1Hand, List<Card> p2Hand,
                                  List<Card> p1Acquired, List<Card> p2Acquired,
                                  List<Card> floor, List<Card> remainingDeck) {}

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(ROOM).block();
    }

    @Test
    @DisplayName("라이브 완주 → cleanup 후 최신 스냅샷 restore + tail replay → 전 저장소 상태 동등")
    void latestSnapshotPlusTailReplayReconstructsFinishedGame() throws JsonProcessingException {
        // 무작위 셔플 게임은 드물게 라운드 1에서 끝날 수 있다(스냅샷 0건) — 그 경우만 재시도
        GameState liveEnd = playUntilMultiRoundGame();
        int finalRound = liveEnd.getRound();
        RoomFinalState liveFinal = captureFinalState();

        // 스냅샷 저장은 락 밖 비동기(fire-and-forget) — 라운드 경계 수만큼 쌓일 때까지 대기
        int expectedSnapshots = finalRound - 1;
        awaitSnapshotCount(expectedSnapshots);

        GameSnapshot snapshot = snapshotRepository.findLatest(ROOM).block();
        assertNotNull(snapshot);
        assertSnapshotShape(snapshot, finalRound);

        // cleanup은 로그 drain + 완료 표시 — 스냅샷과 로그는 저장소에 남는다
        roomCleanupService.cleanupRoomData(ROOM).block();
        List<GameLogRecord> log = gameLogRepository.findAllFromSeq(ROOM, 1).collectList().block();
        assertSeqAlignsWithRoundStart(log, snapshot.seq());

        // 복구 경로 — 스냅샷 restore 후 seq 이후 레코드만 replay (replay 길이가 스냅샷으로 상한된다)
        List<GameLogRecord> tail = log.stream().filter(r -> r.seq() > snapshot.seq()).toList();
        assertTrue(tail.size() < log.size() - 1, "tail replay가 전체 커맨드 replay보다 짧아야 한다");
        gameSnapshotService.restore(snapshot).block();
        tail.forEach(this::execute);

        assertEquals(liveFinal, captureFinalState(), "restore + tail replay 최종 상태 불일치");
    }

    private void assertSnapshotShape(GameSnapshot snapshot, int finalRound) {
        GameState state = snapshot.gameState();
        assertEquals(GamePhase.IN_PROGRESS, state.getPhase(), "스냅샷은 라운드 시작 시점이어야 한다");
        assertEquals(1, state.getCurrentTurn());
        assertEquals(finalRound, state.getRound(), "최신 스냅샷은 마지막 라운드 경계");

        // 카드 보존 — 6개 컬렉션의 서로소 합집합이 전체 덱과 일치해야 한다 (torn snapshot이면 깨진다)
        List<Card> all = new ArrayList<>();
        all.addAll(snapshot.p1Hand());
        all.addAll(snapshot.p2Hand());
        all.addAll(snapshot.floorCards());
        all.addAll(snapshot.hiddenDeck());
        all.addAll(snapshot.p1Acquired());
        all.addAll(snapshot.p2Acquired());
        Set<Card> distinct = new HashSet<>(all);
        assertEquals(all.size(), distinct.size(), "스냅샷 컬렉션 간 카드 중복");
        assertEquals(EnumSet.allOf(Card.class), distinct, "스냅샷에 유실된 카드가 있다");
    }

    private void assertSeqAlignsWithRoundStart(List<GameLogRecord> log, long snapshotSeq) {
        GameLogRecord atSeq = log.stream().filter(r -> r.seq() == snapshotSeq).findFirst().orElseThrow();
        assertEquals(GamePhase.IN_PROGRESS, atSeq.nextPhase(), "스냅샷 seq는 라운드를 연 커맨드여야 한다");
    }

    private GameState playUntilMultiRoundGame() {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            GameState created = createRoom();
            preGameService.distributeCards(created).block();
            playLiveGame();
            GameState end = gameStateRepository.findById(ROOM).block();
            if (end.getRound() >= 2) {
                return end;
            }
            roomCleanupService.cleanupRoomData(ROOM).block();
        }
        return fail(MAX_ATTEMPTS + "번 연속 라운드 1 종료 — 통계적으로 비정상");
    }

    private void awaitSnapshotCount(int expected) {
        InMemoryGameSnapshotRepository store = (InMemoryGameSnapshotRepository) snapshotRepository;
        for (int i = 0; i < 100 && store.count(ROOM) < expected; i++) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(e);
            }
        }
        assertEquals(expected, store.count(ROOM), "스냅샷 수 = 라운드 경계 수(최종 라운드 - 1)");
    }

    private GameState createRoom() {
        GameState state = GameState.builder()
                .roomId(ROOM)
                .leadingPlayer(1)
                .currentTurn(1)
                .round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();
        return state;
    }

    /** 상태를 보고 커맨드를 결정·실행 — 자동플레이와 같은 정책(항상 0번, 첫 기회 GO 이후 STOP) */
    private void playLiveGame() {
        for (int count = 0; count < MAX_COMMANDS; count++) {
            GameState state = gameStateRepository.findById(ROOM).block();
            switch (state.getPhase()) {
                case IN_PROGRESS ->
                        gamePlayService.executeNormalSubmit(ROOM, state.getCurrentPlayer(), 0, null).block();
                case AWAITING_FLOOR_CARD_CHOICE ->
                        gamePlayService.executeFloorSelection(ROOM, state.getChoiceInfo().getPlayerNumToChoose(), 0, null).block();
                case AWAITING_GO_STOP_CHOICE -> {
                    Player actor = state.getCurrentPlayer();
                    gamePlayService.executeGoStop(ROOM, actor, state.getPlayerState(actor).getGo() == 0, null).block();
                }
                case END -> {
                    return;
                }
                default -> fail("예상 밖 phase: " + state.getPhase());
            }
        }
        fail("게임이 " + MAX_COMMANDS + " 커맨드 안에 완주되지 않았다");
    }

    private void execute(GameLogRecord record) {
        switch (record.type()) {
            case NORMAL_SUBMIT ->
                    gamePlayService.executeNormalSubmit(ROOM, record.player(), record.cardIndex(), null).block();
            case FLOOR_SELECT ->
                    gamePlayService.executeFloorSelection(ROOM, record.player(), record.cardIndex(), null).block();
            case GO_STOP ->
                    gamePlayService.executeGoStop(ROOM, record.player(), record.go(), null).block();
            case DECK_INIT -> fail("스냅샷 이후 tail에 DECK_INIT이 있으면 안 된다");
        }
    }

    private RoomFinalState captureFinalState() throws JsonProcessingException {
        GameState state = gameStateRepository.findById(ROOM).block();
        assertEquals(GamePhase.END, state.getPhase(), "게임이 완주(END)돼야 한다");
        return new RoomFinalState(
                objectMapper.writeValueAsString(state),
                installedCardRepository.getPlayerCards(ROOM, Player.PLAYER_1).block(),
                installedCardRepository.getPlayerCards(ROOM, Player.PLAYER_2).block(),
                acquiredCardRepository.getAllCards(ROOM, 1).block(),
                acquiredCardRepository.getAllCards(ROOM, 2).block(),
                installedCardRepository.getAllRevealedCards(ROOM).block(),
                installedCardRepository.getHiddenCards(ROOM).block());
    }
}
