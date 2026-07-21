package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;
import static org.junit.jupiter.api.Assertions.*;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@DisplayName("고/스톱 선택 자동플레이 통합 테스트")
class AutoPlayGoStopChoiceTest {

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
    @Autowired RoomCleanupService roomCleanupService;

    private long roomId;

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(roomId).block();
    }

    @Test
    @DisplayName("점수 조건을 채운 카드 제출은 고/스톱 대기 phase로 진입하고, 타임아웃 시 자동 STOP으로 게임이 종료된다")
    void submitReachingGoStopEntersChoicePhaseAndAutoStops() throws Exception {
        roomId = 920_001L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();
        // 광 5장 = 15점 → 제출 턴 완료 시 canGoStop
        acquiredCardRepository.addCards(roomId, 1, List.of(Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.NOV_1, Card.DEC_1)).block();
        installedCardRepository.savePlayerCards(List.of(Card.FEB_1), roomId, Player.PLAYER_1).block();
        installedCardRepository.saveHiddenCard(List.of(Card.MAR_3), roomId).block();

        turnFlowService.processNormalSubmit(roomId, state, Player.PLAYER_1, 0,
                () -> autoPlayScheduler.cancelAutoPlay(roomId), autoPlayScheduler).block();

        GameState afterSubmit = gameStateRepository.findById(roomId).block();
        assertEquals(GamePhase.AWAITING_GO_STOP_CHOICE, afterSubmit.getPhase());
        assertEquals(1, afterSubmit.getCurrentTurn(), "고/스톱 대기 중엔 턴이 유지돼야 한다");

        // 12초를 기다리는 대신 같은 TurnStep의 즉시 발사 타이머로 교체 (원자적 교체 검증 겸)
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.AWAITING_GO_STOP_CHOICE);

        // 자동 STOP → gameOver → 빈 방 상태로 초기화
        GameState result = awaitState(gs -> gs.getPhase() == GamePhase.NONE, 5000);
        assertNotNull(result);
        assertEquals(GamePhase.NONE, result.getPhase());
    }

    @Test
    @DisplayName("고/스톱 대기 중 낡은 카드 제출 타이머는 등록 교체도 발사 실행도 하지 못한다")
    void staleSubmitTimerCannotDisruptGoStopWait() throws Exception {
        roomId = 920_002L;
        seedGoStopPendingRoom();

        // 고/스톱 타이머가 정상 등록된 상태 (3초 뒤 발사 예정)
        long goStopDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(3000);
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, goStopDeadline, GamePhase.AWAITING_GO_STOP_CHOICE);

        // 같은 (round, turn)의 제출 타이머 등록이 뒤늦게 도착 — 제출 < 고/스톱 순서이므로 교체되면 안 된다
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.IN_PROGRESS);

        Thread.sleep(1500);
        assertEquals(GamePhase.AWAITING_GO_STOP_CHOICE, gameStateRepository.findById(roomId).block().getPhase(),
                "낡은 제출 타이머가 발사돼도 phase 재검증으로 무시돼야 한다");

        // 고/스톱 타이머가 살아남아 3초 뒤 자동 STOP이 실행돼야 한다
        GameState result = awaitState(gs -> gs.getPhase() == GamePhase.NONE, 6000);
        assertNotNull(result);
        assertEquals(GamePhase.NONE, result.getPhase());
    }

    @Test
    @DisplayName("GO 선택 시 고 횟수가 오르고 턴이 상대에게 넘어간다")
    void goProceedsToNextTurn() {
        roomId = 920_003L;
        seedGoStopPendingRoom();

        turnFlowService.processGoStopChoice(roomId, gameStateRepository.findById(roomId).block(),
                Player.PLAYER_1, true, null, autoPlayScheduler).block();

        GameState result = gameStateRepository.findById(roomId).block();
        assertEquals(GamePhase.IN_PROGRESS, result.getPhase());
        assertEquals(2, result.getCurrentTurn());
        assertEquals(1, result.getPlayerState(Player.PLAYER_1).getGo());
        assertEquals(7, result.getPlayerState(Player.PLAYER_1).getGoScore(), "고 선언 시점 점수가 기록돼야 한다");
    }

    @Test
    @DisplayName("고/스톱 대기 phase가 아니면 선택 요청이 거부된다")
    void rejectsChoiceOutsideGoStopPhase() {
        roomId = 920_004L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();

        WebSocketBusinessException e = assertThrows(WebSocketBusinessException.class,
                () -> turnFlowService.processGoStopChoice(roomId, state, Player.PLAYER_1, true, null, autoPlayScheduler).block());
        assertEquals(INVALID_GAME_PHASE, e.getWebsocketErrorCode());
    }

    private void seedGoStopPendingRoom() {
        GameState state = GameState.builder()
                .roomId(roomId)
                .leadingPlayer(1)
                .currentTurn(1)   // leadingPlayer == currentTurn → currentPlayer = PLAYER_1
                .round(1)
                .phase(GamePhase.AWAITING_GO_STOP_CHOICE)
                .player1(PlayerState.builder().score(7).build())
                .build();
        gameStateRepository.create(state).block();
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
