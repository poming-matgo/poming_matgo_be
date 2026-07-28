package com.pomingmatgo.gameservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@DisplayName("결정성 감사: 같은 덱 + 같은 커맨드 replay가 같은 최종 상태를 만든다")
class DeterminismReplayTest {

    @Autowired GamePlayService gamePlayService;
    @Autowired PreGameService preGameService;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired InstalledCardRepository installedCardRepository;
    @Autowired AcquiredCardRepository acquiredCardRepository;
    @Autowired RoomCleanupService roomCleanupService;
    @Autowired ObjectMapper objectMapper;

    private static final long ROOM_LIVE = 940_001L;
    private static final long ROOM_REPLAY = 940_002L;
    private static final int MAX_COMMANDS = 80;

    private enum CommandType { SUBMIT, FLOOR_SELECT, GO_STOP }

    /** 커맨드 로그 레코드의 최소형 — 영속화되는 레코드와 같은 정보량 */
    private record Command(CommandType type, Player player, int cardIndex, boolean go) {}

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(ROOM_LIVE).block();
        roomCleanupService.cleanupRoomData(ROOM_REPLAY).block();
    }

    @Test
    @DisplayName("라이브 완주 게임의 최종 상태 == 커맨드 로그만으로 blind replay한 결과")
    void liveGameFinalStateEqualsBlindReplayResult() throws JsonProcessingException {
        List<Card> deck = fixedDeck();

        setUpRoom(ROOM_LIVE, deck);
        List<Command> commandLog = playLiveGame(ROOM_LIVE);

        setUpRoom(ROOM_REPLAY, deck);
        replayBlindly(ROOM_REPLAY, commandLog);

        GameState liveFinal = gameStateRepository.findById(ROOM_LIVE).block();
        GameState replayFinal = gameStateRepository.findById(ROOM_REPLAY).block();

        assertEquals(GamePhase.END, liveFinal.getPhase(), "라이브 게임이 완주(END)돼야 테스트가 성립한다");
        assertTrue(commandLog.size() >= 5, "커맨드가 실제로 누적돼야 한다: " + commandLog.size());

        assertEquals(normalized(liveFinal), normalized(replayFinal), "GameState 불일치");
        for (Player p : List.of(Player.PLAYER_1, Player.PLAYER_2)) {
            assertEquals(installedCardRepository.getPlayerCards(ROOM_LIVE, p).block(),
                    installedCardRepository.getPlayerCards(ROOM_REPLAY, p).block(), p + " 손패 불일치");
            assertEquals(acquiredCardRepository.getAllCards(ROOM_LIVE, p.getNumber()).block(),
                    acquiredCardRepository.getAllCards(ROOM_REPLAY, p.getNumber()).block(), p + " 획득 카드 불일치");
        }
        assertEquals(installedCardRepository.getAllRevealedCards(ROOM_LIVE).block(),
                installedCardRepository.getAllRevealedCards(ROOM_REPLAY).block(), "바닥 카드 불일치");
        assertEquals(drainDeck(ROOM_LIVE), drainDeck(ROOM_REPLAY), "잔여 더미 불일치");
    }

    /** 시드 고정 셔플 — JVM이 달라도 같은 덱이 재현된다 */
    private List<Card> fixedDeck() {
        List<Card> deck = new ArrayList<>(Arrays.asList(Card.values()));
        Collections.shuffle(deck, new Random(20260731L));
        return deck;
    }

    private void setUpRoom(long roomId, List<Card> deck) {
        gameStateRepository.create(GameState.builder()
                .roomId(roomId)
                .leadingPlayer(1)
                .currentTurn(1)
                .round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build()).block();
        preGameService.distributeCards(roomId, deck).block();
    }

    /** 상태를 보고 커맨드를 결정·실행·기록 — 자동플레이와 같은 정책(항상 0번, 첫 기회 GO 이후 STOP) */
    private List<Command> playLiveGame(long roomId) {
        List<Command> log = new ArrayList<>();
        for (int i = 0; i < MAX_COMMANDS; i++) {
            GameState state = gameStateRepository.findById(roomId).block();
            switch (state.getPhase()) {
                case IN_PROGRESS -> {
                    Command cmd = new Command(CommandType.SUBMIT, state.getCurrentPlayer(), 0, false);
                    execute(roomId, cmd);
                    log.add(cmd);
                }
                case AWAITING_FLOOR_CARD_CHOICE -> {
                    Command cmd = new Command(CommandType.FLOOR_SELECT, state.getChoiceInfo().getPlayerNumToChoose(), 0, false);
                    execute(roomId, cmd);
                    log.add(cmd);
                }
                case AWAITING_GO_STOP_CHOICE -> {
                    Player actor = state.getCurrentPlayer();
                    boolean go = state.getPlayerState(actor).getGo() == 0;
                    Command cmd = new Command(CommandType.GO_STOP, actor, 0, go);
                    execute(roomId, cmd);
                    log.add(cmd);
                }
                case END -> {
                    return log;
                }
                default -> fail("예상 밖 phase: " + state.getPhase());
            }
        }
        fail("게임이 " + MAX_COMMANDS + " 커맨드 안에 완주되지 않았다");
        return log;
    }

    /** 상태를 전혀 보지 않고 기록된 커맨드만 순서대로 적용한다 */
    private void replayBlindly(long roomId, List<Command> commandLog) {
        commandLog.forEach(cmd -> execute(roomId, cmd));
    }

    private void execute(long roomId, Command cmd) {
        switch (cmd.type()) {
            case SUBMIT -> gamePlayService.executeNormalSubmit(roomId, cmd.player(), cmd.cardIndex(), null).block();
            case FLOOR_SELECT -> gamePlayService.executeFloorSelection(roomId, cmd.player(), cmd.cardIndex(), null).block();
            case GO_STOP -> gamePlayService.executeGoStop(roomId, cmd.player(), cmd.go(), null).block();
        }
    }

    private List<Card> drainDeck(long roomId) {
        List<Card> remaining = new ArrayList<>();
        Card card;
        while ((card = installedCardRepository.drawTopCard(roomId).block()) != null) {
            remaining.add(card);
        }
        return remaining;
    }

    private String normalized(GameState state) throws JsonProcessingException {
        return objectMapper.writeValueAsString(state.toBuilder().roomId(0L).build());
    }
}
