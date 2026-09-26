package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.api.handler.websocket.WsGameHandler;
import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
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

    @Autowired GameMessageSender gameMessageSender;
    @Autowired WsGameHandler wsGameHandler;
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

        String normalKey = InFlightManager.normalKey(roomId, 1);
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
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();
        installedCardRepository.savePlayerCards(List.of(Card.JAN_3), roomId, Player.PLAYER_1).block();
        // 바닥에 같은 달(1월) 2장 → 제출 시 선택 유발
        installedCardRepository.saveRevealedCard(List.of(Card.JAN_1, Card.JAN_2), roomId).block();
        installedCardRepository.saveHiddenCard(List.of(Card.FEB_3), roomId).block();

        turnFlowService.processNormalSubmit(roomId, Player.PLAYER_1, 0,
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

    @Test
    @DisplayName("지연 도착한 제출 타이머 '등록'이 이미 등록된 선택 타이머를 파괴하지 못한다")
    void staleSubmitRegistrationCannotReplaceChoiceTimer() throws Exception {
        roomId = 910_005L;
        seedChoicePendingRoom();

        // 선택 타이머가 정상 등록된 상태 (3초 뒤 발사 예정)
        long choiceDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(3000);
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, choiceDeadline, GamePhase.AWAITING_FLOOR_CARD_CHOICE);

        // 같은 (round, turn)의 제출 타이머 등록이 뒤늦게 도착 — 같은 턴 안에서 제출 < 선택 순서이므로 교체되면 안 된다
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.IN_PROGRESS);

        // 선택 타이머가 살아남아 3초 뒤 자동 선택이 실행돼야 한다
        GameState result = awaitState(gs -> gs.getPhase() == GamePhase.IN_PROGRESS && gs.getCurrentTurn() == 2, 6000);
        assertNotNull(result);
        assertEquals(2, result.getCurrentTurn());
    }

    @ParameterizedTest
    @CsvSource({"false,-1", "false,99", "true,-1", "true,99"})
    @DisplayName("잘못된 제출/선택 요청 뒤에도 기존 마감에 자동플레이가 진행된다")
    void invalidRequestPreservesAutoPlay(boolean floorChoice, int invalidIndex) throws Exception {
        roomId = 910_006L;
        if (floorChoice) {
            seedChoicePendingRoom();
        } else {
            gameStateRepository.create(GameState.builder()
                    .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                    .phase(GamePhase.IN_PROGRESS).build()).block();
            installedCardRepository.savePlayerCards(List.of(Card.JAN_3), roomId, Player.PLAYER_1).block();
            installedCardRepository.saveHiddenCard(List.of(Card.FEB_3), roomId).block();
        }
        GameState before = gameStateRepository.findById(roomId).block();
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1,
                System.nanoTime() + TimeUnit.SECONDS.toNanos(1), before.getPhase());
        var event = new RequestEvent<NormalSubmitReq>();
        event.setSubCategory(floorChoice
                ? SubCategory.FLOOR_SELECT
                : SubCategory.NORMAL_SUBMIT);
        event.setData(new NormalSubmitReq(invalidIndex));

        var error = assertThrows(WebSocketBusinessException.class,
                () -> wsGameHandler.handleGameEvent(event, before, Player.PLAYER_1).block());
        assertEquals(WebSocketErrorCode.INVALID_CARD,
                error.getWebsocketErrorCode());
        assertEquals(1, gameStateRepository.findById(roomId).block().getCurrentTurn());
        GameState after = awaitState(gs -> gs.getCurrentTurn() == 2, 3500);
        assertNotNull(after);
        assertEquals(2, after.getCurrentTurn(), "에러 응답 뒤에도 기존 타이머가 턴을 진행해야 한다");
    }

    @Test
    @DisplayName("이전 턴 송신이 늦어도 타이머는 상태 전이 순서로 등록되고 선택 타이머를 덮어쓰지 않는다")
    void delayedTurnAnnouncementDoesNotReorderTimers() throws Exception {
        roomId = 910_007L;
        gameStateRepository.create(GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS).build()).block();
        installedCardRepository.savePlayerCards(List.of(Card.JAN_3), roomId, Player.PLAYER_1).block();
        installedCardRepository.savePlayerCards(List.of(Card.FEB_3), roomId, Player.PLAYER_2).block();
        installedCardRepository.saveRevealedCard(List.of(Card.FEB_1, Card.FEB_2), roomId).block();
        installedCardRepository.saveHiddenCard(List.of(Card.MAR_1, Card.APR_1), roomId).block();

        // P2는 턴 안내를 받았지만 P1 쪽 송신 완료는 아직인 상황을 재현한다.
        var announcementCompleted = reactor.core.publisher.Sinks.<Void>empty();
        Mockito.doReturn(announcementCompleted.asMono()).when(gameMessageSender)
                .sendTurnInfo(Mockito.argThat(state -> state.getRoomId() == roomId), Mockito.anyLong());
        TurnScheduler recordingScheduler = Mockito.mock(TurnScheduler.class);
        Mockito.doAnswer(invocation -> {
            GamePhase phase = invocation.getArgument(5);
            long deadline = phase == GamePhase.AWAITING_FLOOR_CARD_CHOICE
                    ? System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
                    : invocation.<Long>getArgument(4);
            autoPlayScheduler.scheduleAutoPlay(invocation.getArgument(0), invocation.getArgument(1),
                    invocation.getArgument(2), invocation.getArgument(3), deadline, phase);
            return null;
        }).when(recordingScheduler).scheduleAutoPlay(Mockito.anyLong(), Mockito.anyInt(), Mockito.anyInt(),
                Mockito.any(Player.class), Mockito.anyLong(), Mockito.any(GamePhase.class));

        try {
            reactor.test.StepVerifier.create(turnFlowService.processNormalSubmit(roomId, Player.PLAYER_1, 0,
                            () -> autoPlayScheduler.cancelAutoPlay(roomId), recordingScheduler))
                    .then(() -> {
                        GameState nextTurn = gameStateRepository.findById(roomId).block();
                        assertEquals(Player.PLAYER_2, nextTurn.getCurrentPlayer());
                        assertEquals(GamePhase.IN_PROGRESS, nextTurn.getPhase());
                        Mockito.verify(recordingScheduler).scheduleAutoPlay(Mockito.eq(roomId), Mockito.eq(1), Mockito.eq(2),
                                Mockito.eq(Player.PLAYER_2), Mockito.anyLong(), Mockito.eq(GamePhase.IN_PROGRESS));

                        // 상태 전이는 순차 실행하고, P1의 송신 후처리가 남은 상태에서 P2가 유효한 제출을 한다.
                        turnFlowService.processNormalSubmit(roomId, Player.PLAYER_2, 0,
                                () -> autoPlayScheduler.cancelAutoPlay(roomId), recordingScheduler)
                                .block(Duration.ofSeconds(2));
                        assertEquals(GamePhase.AWAITING_FLOOR_CARD_CHOICE,
                                gameStateRepository.findById(roomId).block().getPhase());
                        assertEquals(reactor.core.publisher.Sinks.EmitResult.OK, announcementCompleted.tryEmitEmpty());
                    })
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));

            var order = Mockito.inOrder(recordingScheduler);
            order.verify(recordingScheduler).scheduleAutoPlay(Mockito.eq(roomId), Mockito.eq(1), Mockito.eq(2),
                    Mockito.eq(Player.PLAYER_2), Mockito.anyLong(), Mockito.eq(GamePhase.IN_PROGRESS));
            order.verify(recordingScheduler).scheduleAutoPlay(Mockito.eq(roomId), Mockito.eq(1), Mockito.eq(2),
                    Mockito.eq(Player.PLAYER_2), Mockito.anyLong(), Mockito.eq(GamePhase.AWAITING_FLOOR_CARD_CHOICE));
            order.verifyNoMoreInteractions();

            Mockito.reset(gameMessageSender);
            GameState afterTimeout = awaitState(state -> state.getRound() == 2, 4000);
            assertEquals(2, afterTimeout.getRound(), "이전 송신 완료 후에도 선택 타이머가 진행해야 한다");
            assertNull(afterTimeout.getChoiceInfo());
        } finally {
            Mockito.reset(gameMessageSender);
        }
    }

    private void seedChoicePendingRoom() {
        GameState state = GameState.builder()
                .roomId(roomId)
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
