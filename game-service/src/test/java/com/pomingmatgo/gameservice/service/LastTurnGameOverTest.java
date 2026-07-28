package com.pomingmatgo.gameservice.service;

import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.scheduler.AutoPlayScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@DisplayName("마지막 턴 게임 종료 판정 통합 테스트")
class LastTurnGameOverTest {

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
    @Autowired GameMessageSender gameMessageSender;

    private long roomId;

    @BeforeEach
    void resetSenderMock() {
        Mockito.clearInvocations(gameMessageSender);
    }

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(roomId).block();
    }

    @Test
    @DisplayName("마지막 턴에 점수를 달성하면 자동 스톱으로 행동자가 승리한다")
    void lastTurnReachingScoreAutoStopsWithActorAsWinner() {
        roomId = 930_001L;
        GameState state = lastTurnState();
        gameStateRepository.create(state).block();
        // 광 5장 = 15점 → 제출 턴 완료 시 canGoStop
        acquiredCardRepository.addCards(roomId, 2, List.of(Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.NOV_1, Card.DEC_1)).block();
        seedHandAndDeck();

        turnFlowService.processNormalSubmit(roomId, Player.PLAYER_2, 0, null, autoPlayScheduler).block();

        Mockito.verify(gameMessageSender).sendGameOverMessage(any(), eq(Player.PLAYER_2), any());
        assertEquals(GamePhase.NONE, gameStateRepository.findById(roomId).block().getPhase(),
                "게임 종료 후 빈 방 상태로 초기화돼야 한다");
    }

    @Test
    @DisplayName("마지막 턴에 점수 미달이면 무승부(PLAYER_NOTHING)로 종료된다")
    void lastTurnWithoutScoreEndsInDraw() {
        roomId = 930_002L;
        GameState state = lastTurnState();
        gameStateRepository.create(state).block();
        seedHandAndDeck();

        turnFlowService.processNormalSubmit(roomId, Player.PLAYER_2, 0, null, autoPlayScheduler).block();

        Mockito.verify(gameMessageSender).sendGameOverMessage(any(), eq(Player.PLAYER_NOTHING), any());
        assertEquals(GamePhase.NONE, gameStateRepository.findById(roomId).block().getPhase(),
                "무승부 종료 후에도 빈 방 상태로 초기화돼야 한다");
    }

    /** round 10, turn 2 = 마지막 턴. leadingPlayer(1) != currentTurn(2) → currentPlayer = PLAYER_2 */
    private GameState lastTurnState() {
        return GameState.builder()
                .roomId(roomId)
                .leadingPlayer(1)
                .currentTurn(2)
                .round(10)
                .phase(GamePhase.IN_PROGRESS)
                .build();
    }

    /** 낸 카드(2월)와 뒤집은 카드(3월)가 빈 바닥에서 아무것도 못 먹는 구성 — 점수 변화 없음 */
    private void seedHandAndDeck() {
        installedCardRepository.savePlayerCards(List.of(Card.FEB_1), roomId, Player.PLAYER_2).block();
        installedCardRepository.saveHiddenCard(List.of(Card.MAR_3), roomId).block();
    }
}
