package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.api.response.websocket.GameMessageSender;
import com.pomingmatgo.gameservice.domain.ChoiceInfo;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@DisplayName("바닥 카드 선택 자동플레이 통합 테스트")
class AutoPlayFloorSelectionTest {

    /** 세션 없이 도메인 흐름만 검증하기 위해 메시지 전송은 no-op으로 대체 */
    @TestConfiguration
    static class NoopSenderConfig {
        @Bean
        @Primary
        GameMessageSender noopGameMessageSender() {
            return Mockito.mock(GameMessageSender.class, invocation -> Mono.empty());
        }
    }

    @Autowired AutoPlayScheduler autoPlayScheduler;
    @Autowired TurnFlowService turnFlowService;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired InstalledCardRepository installedCardRepository;
    @Autowired AcquiredCardRepository acquiredCardRepository;
    @Autowired InFlightManager inFlightManager;
    @Autowired RoomCleanupService roomCleanupService;

    private long roomId;

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(roomId).block();
    }

    @Test
    @DisplayName("선택 타임아웃 시 자동으로 바닥 카드를 선택하고 턴을 넘긴다")
    void autoSelectsFloorCardOnTimeout() throws Exception {
        roomId = 910_001L;
        seedChoicePendingRoom();

        // deadline을 현재 시각으로 → 즉시(100ms) 발사
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.AWAITING_FLOOR_CARD_CHOICE);

        GameState result = awaitState(gs -> gs.getPhase() == GamePhase.IN_PROGRESS && gs.getCurrentTurn() == 2, 5000);

        assertNotNull(result);
        assertEquals(GamePhase.IN_PROGRESS, result.getPhase());
        assertEquals(2, result.getCurrentTurn());
        assertNull(result.getChoiceInfo());

        // 0번 선택지(JAN_1) + 제출했던 카드(JAN_3) 획득
        List<Card> acquired = acquiredCardRepository.getAllCards(roomId, 1).block();
        assertTrue(acquired.containsAll(List.of(Card.JAN_1, Card.JAN_3)), "획득 카드: " + acquired);
    }

    @Test
    @DisplayName("선택 대기 중에는 낡은 카드 제출 타이머가 발사돼도 아무 일도 일어나지 않는다")
    void staleSubmitTimerIsIgnoredDuringChoicePhase() throws Exception {
        roomId = 910_002L;
        seedChoicePendingRoom();

        // 카드 제출(IN_PROGRESS)용으로 등록됐던 타이머가 선택 대기 중 뒤늦게 발사된 상황
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.IN_PROGRESS);

        Thread.sleep(1500);

        GameState state = gameStateRepository.findById(roomId).block();
        assertEquals(GamePhase.AWAITING_FLOOR_CARD_CHOICE, state.getPhase());
        assertNotNull(state.getChoiceInfo());
    }

    @Test
    @DisplayName("사용자 요청이 진행 중이면 자동 선택이 양보하고, 끝나면 재시도한다")
    void yieldsToInFlightUserRequest() throws Exception {
        roomId = 910_003L;
        seedChoicePendingRoom();

        String normalKey = "IN_FLIGHT:NORMAL:ROOM:" + roomId + ":PLAYER:1";
        String token = "test-token";
        inFlightManager.trySetFlag(normalKey, token, Duration.ofSeconds(30)).block();

        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.AWAITING_FLOOR_CARD_CHOICE);

        Thread.sleep(1500);
        assertEquals(GamePhase.AWAITING_FLOOR_CARD_CHOICE, gameStateRepository.findById(roomId).block().getPhase(),
                "NORMAL 플래그가 켜져 있는 동안은 양보해야 한다");

        inFlightManager.deleteFlag(normalKey, token).block();

        GameState result = awaitState(gs -> gs.getPhase() == GamePhase.IN_PROGRESS, 4000);
        assertEquals(GamePhase.IN_PROGRESS, result.getPhase(), "플래그 해제 후 1초 주기 재시도에서 실행돼야 한다");
    }

    @Test
    @DisplayName("카드 제출이 선택을 유발하면 선택 타이머가 등록되고, 타임아웃 시 자동 선택된다")
    void submitLeadingToChoiceSchedulesChoiceTimer() throws Exception {
        roomId = 910_004L;
        GameState state = GameState.builder()
                .roomId(roomId).gameStarted(true).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();
        installedCardRepository.savePlayerCards(List.of(Card.JAN_3), roomId, Player.PLAYER_1).block();
        // 바닥에 같은 달(1월) 2장 → 제출 시 선택 유발
        installedCardRepository.saveRevealedCard(List.of(Card.JAN_1, Card.JAN_2), roomId).block();
        installedCardRepository.saveHiddenCard(List.of(Card.FEB_3), roomId).block();

        turnFlowService.processNormalSubmit(roomId, state, Player.PLAYER_1, 0,
                () -> autoPlayScheduler.cancelAutoPlay(roomId), autoPlayScheduler).block();

        GameState afterSubmit = gameStateRepository.findById(roomId).block();
        assertEquals(GamePhase.AWAITING_FLOOR_CARD_CHOICE, afterSubmit.getPhase());

        // 12초를 기다리는 대신 같은 (round, turn) 시퀀스의 즉시 발사 타이머로 교체 (원자적 교체 검증 겸)
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.AWAITING_FLOOR_CARD_CHOICE);

        GameState result = awaitState(gs -> gs.getPhase() == GamePhase.IN_PROGRESS && gs.getCurrentTurn() == 2, 5000);
        assertNotNull(result);
        assertEquals(2, result.getCurrentTurn());

        // 선택지는 Set에서 복원돼 순서가 비결정적 → 제출 카드 + 1월 카드 중 하나를 획득했는지로 검증
        List<Card> acquired = acquiredCardRepository.getAllCards(roomId, 1).block();
        assertTrue(acquired.contains(Card.JAN_3), "획득 카드: " + acquired);
        assertTrue(acquired.contains(Card.JAN_1) || acquired.contains(Card.JAN_2), "획득 카드: " + acquired);

        // 선택 처리 중 뒤집었던 카드(FEB_3)는 바닥에 놓여야 한다
        List<Card> floorFeb = installedCardRepository.getRevealedCardByMonth(roomId, 2).block();
        assertTrue(floorFeb.contains(Card.FEB_3), "2월 바닥: " + floorFeb);
    }

    private void seedChoicePendingRoom() {
        GameState state = GameState.builder()
                .roomId(roomId)
                .gameStarted(true)
                .leadingPlayer(1)
                .currentTurn(1)   // leadingPlayer == currentTurn → currentPlayer = PLAYER_1
                .round(1)
                .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                .choiceInfo(ChoiceInfo.builder()
                        .playerNumToChoose(Player.PLAYER_1)
                        .submittedCard(Card.JAN_3)
                        .selectableCards(List.of(Card.JAN_1, Card.JAN_2))
                        .build())
                .build();
        gameStateRepository.create(state).block();
        installedCardRepository.saveRevealedCard(List.of(Card.JAN_1, Card.JAN_2), roomId).block();
    }

    private GameState awaitState(Predicate<GameState> condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        GameState last = null;
        while (System.nanoTime() < deadline) {
            last = gameStateRepository.findById(roomId).block();
            if (last != null && condition.test(last)) {
                return last;
            }
            Thread.sleep(100);
        }
        return last;
    }
}
