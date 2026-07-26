package com.pomingmatgo.gameservice.service;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.domain.messaging.LeadSelectionRes;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameFlowService;
import com.pomingmatgo.gameservice.domain.service.matgo.PreGameService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@DisplayName("초기 바닥 같은 월 4장 무승부 통합 테스트")
class PreGameFloorDrawTest {

    /** 배분은 무작위라 검증 불가 — 카드 구성을 고정하고 메시지 전송은 no-op으로 대체한다 */
    @TestConfiguration
    static class StubConfig {
        @Bean
        @Primary
        GameMessageSender noopGameMessageSender() {
            return Mockito.mock(GameMessageSender.class, invocation -> Mono.empty());
        }

        @Bean
        @Primary
        PreGameService stubPreGameService() {
            return Mockito.mock(PreGameService.class);
        }
    }

    @Autowired PreGameFlowService preGameFlowService;
    @Autowired PreGameService preGameService;
    @Autowired GameMessageSender gameMessageSender;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired RoomCleanupService roomCleanupService;

    private long roomId;

    @BeforeEach
    void resetMocks() {
        Mockito.reset(preGameService);
        Mockito.clearInvocations(gameMessageSender);
    }

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(roomId).block();
    }

    @Test
    @DisplayName("바닥에 같은 월 4장이 깔리면 첫 턴을 시작하지 않고 무승부로 끝난다")
    void fourOfSameMonthOnFloorEndsInDraw() {
        roomId = 940_001L;
        stubLeaderSelection();
        stubDeal(List.of(Card.JAN_1, Card.JAN_2, Card.JAN_3, Card.JAN_4,
                Card.FEB_1, Card.MAR_1, Card.APR_1, Card.MAY_1));
        gameStateRepository.create(pendingStartState()).block();

        preGameFlowService.processLeaderSelection(pendingStartState(), Player.PLAYER_1, 0).block();

        Mockito.verify(gameMessageSender).sendGameOverMessage(any(), eq(Player.PLAYER_NOTHING), any());
        Mockito.verify(preGameService, never()).setFirstTurn(any());
        assertEquals(GamePhase.NONE, gameStateRepository.findById(roomId).block().getPhase(),
                "무승부 종료 후 빈 방 상태로 초기화돼야 한다");
    }

    @Test
    @DisplayName("같은 월이 3장까지면 평소대로 첫 턴이 시작된다")
    void threeOfSameMonthOnFloorStartsGame() {
        roomId = 940_002L;
        stubLeaderSelection();
        stubDeal(List.of(Card.JAN_1, Card.JAN_2, Card.JAN_3, Card.FEB_1,
                Card.MAR_1, Card.APR_1, Card.MAY_1, Card.JUN_1));
        gameStateRepository.create(pendingStartState()).block();

        preGameFlowService.processLeaderSelection(pendingStartState(), Player.PLAYER_1, 0).block();

        Mockito.verify(gameMessageSender, never()).sendGameOverMessage(any(), any(), any());
        Mockito.verify(preGameService).setFirstTurn(any());
        assertEquals(GamePhase.IN_PROGRESS, gameStateRepository.findById(roomId).block().getPhase());
    }

    private void stubLeaderSelection() {
        LeadSelectionRes leadSelectionRes = new LeadSelectionRes();
        leadSelectionRes.setLeadPlayer(1);

        when(preGameService.selectLeaderCard(anyLong(), any(), anyInt())).thenReturn(Mono.empty());
        when(preGameService.checkAllSelected(anyLong())).thenReturn(Mono.just(true));
        when(preGameService.getLeadSelectionRes(anyLong())).thenReturn(Mono.just(leadSelectionRes));
        when(preGameService.hasChongtong(anyLong(), any())).thenReturn(Mono.just(false));
        when(preGameService.setFirstTurn(any())).thenAnswer(invocation -> {
            GameState state = invocation.getArgument(0);
            GameState started = state.toBuilder()
                    .round(1).currentTurn(1).phase(GamePhase.IN_PROGRESS)
                    .build();
            return gameStateRepository.save(started).thenReturn(started);
        });
    }

    /** 판정 대상은 바닥뿐이라 손패/더미는 비워 둔다 */
    private void stubDeal(List<Card> floorCards) {
        when(preGameService.distributeCards(anyLong()))
                .thenReturn(Mono.just(new InstalledCard(List.of(), List.of(), floorCards, List.of())));
    }

    private GameState pendingStartState() {
        return GameState.builder()
                .roomId(roomId)
                .leadingPlayer(1)
                .phase(GamePhase.DETERMINING_STARTING_PLAYER)
                .build();
    }
}
