package com.pomingmatgo.gameservice.gamelog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameLogRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InMemoryGameLogRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.GamePlayService;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude="
                + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration",
        "game.log.store=in-memory"})
@DisplayName("커맨드 로그: 영속화된 로그만으로 완주 게임이 재구성된다")
class GameLogReplayTest {

    @Autowired GamePlayService gamePlayService;
    @Autowired PreGameService preGameService;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired InstalledCardRepository installedCardRepository;
    @Autowired AcquiredCardRepository acquiredCardRepository;
    @Autowired RoomCleanupService roomCleanupService;
    @Autowired GameLogRepository gameLogRepository;
    @Autowired ObjectMapper objectMapper;

    private static final long ROOM_LIVE = 950_001L;
    private static final long ROOM_REPLAY = 950_002L;
    private static final int MAX_COMMANDS = 80;

    private record RoomFinalState(String gameState, List<Card> p1Hand, List<Card> p2Hand,
                                  List<Card> p1Acquired, List<Card> p2Acquired,
                                  List<Card> floor, List<Card> remainingDeck) {}

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(ROOM_LIVE).block();
        roomCleanupService.cleanupRoomData(ROOM_REPLAY).block();
    }

    @Test
    @DisplayName("라이브 완주(무작위 셔플) → cleanup 후 남은 로그로 blind replay → 전 저장소 상태 동등")
    void persistedLogAloneReconstructsFinishedGame() throws JsonProcessingException {
        // 라이브 경로 — 무작위 셔플이 DECK_INIT 레코드로 고정되는지가 검증 대상이므로 시드 덱을 쓰지 않는다
        createRoom(ROOM_LIVE);
        preGameService.distributeCards(ROOM_LIVE).block();
        int commandCount = playLiveGame(ROOM_LIVE);
        RoomFinalState liveFinal = captureFinalState(ROOM_LIVE);

        // cleanup은 drain + 완료 표시만 — 로그는 저장소에 남아야 한다
        roomCleanupService.cleanupRoomData(ROOM_LIVE).block();
        List<GameLogRecord> log = gameLogRepository.findAllFromSeq(ROOM_LIVE, 1).collectList().block();
        assertTrue(((InMemoryGameLogRepository) gameLogRepository).isCompleted(ROOM_LIVE), "완료 표시 누락");

        assertLogShape(log, commandCount);

        // blind replay — 저장된 로그 외엔 아무것도 보지 않는다 (덱은 DECK_INIT, 커맨드는 이후 레코드)
        createRoom(ROOM_REPLAY);
        preGameService.distributeCards(ROOM_REPLAY, log.get(0).deck()).block();
        log.stream().skip(1).forEach(this::execute);

        assertEquals(liveFinal, captureFinalState(ROOM_REPLAY), "replay 최종 상태 불일치");
    }

    private void assertLogShape(List<GameLogRecord> log, int commandCount) {
        assertEquals(commandCount + 1, log.size(), "레코드 수 = 커맨드 수 + DECK_INIT");

        GameLogRecord deckRecord = log.get(0);
        assertEquals(GameCommandType.DECK_INIT, deckRecord.type());
        assertEquals(1, deckRecord.seq());
        assertEquals(EnumSet.allOf(Card.class), new HashSet<>(deckRecord.deck()), "덱은 전체 카드의 순열이어야 한다");

        for (int i = 0; i < log.size(); i++) {
            assertEquals(i + 1, log.get(i).seq(), "seq 결번 또는 역전");
        }

        List<GameLogRecord> commands = log.subList(1, log.size());
        assertEquals(GamePhase.IN_PROGRESS, commands.get(0).prevPhase(), "첫 커맨드는 첫 턴 제출");
        for (int i = 1; i < commands.size(); i++) {
            assertEquals(commands.get(i - 1).nextPhase(), commands.get(i).prevPhase(),
                    "phase 사슬 단절: seq " + commands.get(i).seq());
            assertNull(commands.get(i).deck(), "커맨드 레코드에 덱이 실리면 안 된다");
        }
        assertEquals(GamePhase.END, commands.get(commands.size() - 1).nextPhase(), "마지막 커맨드는 게임을 끝내야 한다");
    }

    private void createRoom(long roomId) {
        gameStateRepository.create(GameState.builder()
                .roomId(roomId)
                .leadingPlayer(1)
                .currentTurn(1)
                .round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build()).block();
    }

    /** 상태를 보고 커맨드를 결정·실행 — 자동플레이와 같은 정책(항상 0번, 첫 기회 GO 이후 STOP) */
    private int playLiveGame(long roomId) {
        for (int count = 0; count < MAX_COMMANDS; count++) {
            GameState state = gameStateRepository.findById(roomId).block();
            switch (state.getPhase()) {
                case IN_PROGRESS ->
                        gamePlayService.executeNormalSubmit(roomId, state.getCurrentPlayer(), 0, null).block();
                case AWAITING_FLOOR_CARD_CHOICE ->
                        gamePlayService.executeFloorSelection(roomId, state.getChoiceInfo().getPlayerNumToChoose(), 0, null).block();
                case AWAITING_GO_STOP_CHOICE -> {
                    Player actor = state.getCurrentPlayer();
                    gamePlayService.executeGoStop(roomId, actor, state.getPlayerState(actor).getGo() == 0, null).block();
                }
                case END -> {
                    return count;
                }
                default -> fail("예상 밖 phase: " + state.getPhase());
            }
        }
        return fail("게임이 " + MAX_COMMANDS + " 커맨드 안에 완주되지 않았다");
    }

    private void execute(GameLogRecord record) {
        switch (record.type()) {
            case NORMAL_SUBMIT ->
                    gamePlayService.executeNormalSubmit(ROOM_REPLAY, record.player(), record.cardIndex(), null).block();
            case FLOOR_SELECT ->
                    gamePlayService.executeFloorSelection(ROOM_REPLAY, record.player(), record.cardIndex(), null).block();
            case GO_STOP ->
                    gamePlayService.executeGoStop(ROOM_REPLAY, record.player(), record.go(), null).block();
            case DECK_INIT -> fail("DECK_INIT은 replay 커맨드가 아니다");
        }
    }

    private RoomFinalState captureFinalState(long roomId) throws JsonProcessingException {
        GameState state = gameStateRepository.findById(roomId).block();
        assertEquals(GamePhase.END, state.getPhase(), "게임이 완주(END)돼야 한다");
        return new RoomFinalState(
                objectMapper.writeValueAsString(state.toBuilder().roomId(0L).build()),
                installedCardRepository.getPlayerCards(roomId, Player.PLAYER_1).block(),
                installedCardRepository.getPlayerCards(roomId, Player.PLAYER_2).block(),
                acquiredCardRepository.getAllCards(roomId, 1).block(),
                acquiredCardRepository.getAllCards(roomId, 2).block(),
                installedCardRepository.getAllRevealedCards(roomId).block(),
                drainDeck(roomId));
    }

    private List<Card> drainDeck(long roomId) {
        List<Card> remaining = new ArrayList<>();
        Card card;
        while ((card = installedCardRepository.drawTopCard(roomId).block()) != null) {
            remaining.add(card);
        }
        return remaining;
    }
}
