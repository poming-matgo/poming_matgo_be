package com.pomingmatgo.gameservice.service;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.score.Payout;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.service.matgo.SpecialEvent;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.scheduler.AutoPlayScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@DisplayName("세번뻑 즉시 승리 통합 테스트")
class ThreePpeokTest {

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
    @Autowired RoomCleanupService roomCleanupService;
    @Autowired GameMessageSender gameMessageSender;

    private long roomId;

    // 전송 Mono는 cold라 mock의 호출 순서(assembly)와 실제 전송 순서(subscribe)가 다르다 — 구독 시점에 기록해야 전선 순서를 본다
    private final List<String> sentOrder = new CopyOnWriteArrayList<>();

    @BeforeEach
    void resetSenderMock() {
        Mockito.clearInvocations(gameMessageSender);
        sentOrder.clear();
        Mockito.doAnswer(inv -> Mono.fromRunnable(
                        () -> sentOrder.add(((SpecialEvent) inv.getArgument(2)).name())))
                .when(gameMessageSender).sendSpecialEventMessageIfNeeded(anyLong(), any(), any());
        Mockito.doAnswer(inv -> Mono.fromRunnable(() -> sentOrder.add("GAME_OVER")))
                .when(gameMessageSender).sendGameOverMessage(any(), any(), any());
    }

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(roomId).block();
    }

    @Test
    @DisplayName("세 번째 뻑을 낸 플레이어가 점수·배수와 무관하게 7점으로 즉시 승리한다")
    void thirdPpeokWinsWithSevenPoints() {
        roomId = 940_001L;
        // 상대 고 3회 — 일반 정산이면 고박이 붙는 조건
        GameState state = stateWithPpeokCount(2).updatePlayerState(Player.PLAYER_2,
                ps -> ps.toBuilder().go(3).build());
        gameStateRepository.create(state).block();
        seedPpeokDeal();

        turnFlowService.processNormalSubmit(roomId, state, Player.PLAYER_1, 0, null, autoPlayScheduler).block();

        // 클라가 종료 사유를 알 수 있으려면 GAME_OVER 앞에 세번뻑이 나가야 한다
        assertEquals(List.of("PPEOK", "THREE_PPEOK", "GAME_OVER"), sentOrder);
        Mockito.verify(gameMessageSender)
                .sendSpecialEventMessageIfNeeded(roomId, Player.PLAYER_1, SpecialEvent.THREE_PPEOK);

        ArgumentCaptor<Payout> payout = ArgumentCaptor.forClass(Payout.class);
        Mockito.verify(gameMessageSender).sendGameOverMessage(any(), eq(Player.PLAYER_1), payout.capture());
        assertEquals(7, payout.getValue().baseScore(), "획득 카드가 없어도 세번뻑 승리는 7점으로 정산돼야 한다");
        assertEquals(7, payout.getValue().total(), "세번뻑 승리엔 배수가 붙지 않아야 한다");
        assertTrue(payout.getValue().multipliers().isEmpty());
        assertEquals(GamePhase.NONE, gameStateRepository.findById(roomId).block().getPhase(),
                "게임 종료 후 빈 방 상태로 초기화돼야 한다");
    }

    @Test
    @DisplayName("두 번째 뻑까지는 카운터만 누적되고 턴이 이어진다")
    void secondPpeokOnlyAccumulates() {
        roomId = 940_002L;
        GameState state = stateWithPpeokCount(1);
        gameStateRepository.create(state).block();
        seedPpeokDeal();

        turnFlowService.processNormalSubmit(roomId, state, Player.PLAYER_1, 0, null, autoPlayScheduler).block();

        assertEquals(List.of("PPEOK"), sentOrder, "세번뻑/게임 종료 메시지가 나가면 안 된다");
        GameState next = gameStateRepository.findById(roomId).block();
        assertEquals(2, next.getPlayerState(Player.PLAYER_1).getPpeokCount());
        assertEquals(GamePhase.IN_PROGRESS, next.getPhase());
        assertEquals(2, next.getCurrentTurn(), "턴이 상대에게 넘어가야 한다");
    }

    /** leadingPlayer(1) == currentTurn(1) → currentPlayer = PLAYER_1 */
    private GameState stateWithPpeokCount(int ppeokCount) {
        return GameState.builder()
                .roomId(roomId)
                .leadingPlayer(1)
                .currentTurn(1)
                .round(1)
                .phase(GamePhase.IN_PROGRESS)
                .player1(PlayerState.builder().ppeokCount(ppeokCount).build())
                .build();
    }

    /** 낸 카드(2월)와 뒤집은 카드(2월)가 2월 바닥 1장과 겹치는 뻑 구성 — 아무도 못 가져간다 */
    private void seedPpeokDeal() {
        installedCardRepository.savePlayerCards(List.of(Card.FEB_1), roomId, Player.PLAYER_1).block();
        installedCardRepository.saveRevealedCard(List.of(Card.FEB_2), roomId).block();
        installedCardRepository.saveHiddenCard(List.of(Card.FEB_3), roomId).block();
    }
}
