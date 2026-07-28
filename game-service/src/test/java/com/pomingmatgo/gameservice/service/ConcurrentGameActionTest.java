package com.pomingmatgo.gameservice.service;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.handler.websocket.WsRoomHandler;
import com.pomingmatgo.gameservice.domain.ChoiceInfo;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.GamePlayService;
import com.pomingmatgo.gameservice.domain.service.matgo.RoomCleanupService;
import com.pomingmatgo.gameservice.domain.service.matgo.TurnFlowService;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.lock.InFlightManager;
import com.pomingmatgo.gameservice.scheduler.AutoPlayScheduler;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.NOT_YOUR_TURN;
import static org.junit.jupiter.api.Assertions.*;

// redisson-starter 자동 설정은 프로파일과 무관하게 Redis 연결을 시도하므로 테스트에선 제외 (in-memory 프로파일 검증)
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@DisplayName("게임 액션 동시성 방어 통합 테스트")
class ConcurrentGameActionTest {

    /** 세션 없이 도메인 흐름만 검증하기 위해 메시지 전송은 no-op으로 대체 */
    @TestConfiguration
    static class NoopSenderConfig {
        @Bean
        @Primary
        GameMessageSender noopGameMessageSender() {
            return Mockito.mock(GameMessageSender.class, invocation -> Mono.empty());
        }
    }

    @Autowired TurnFlowService turnFlowService;
    @Autowired GamePlayService gamePlayService;
    @Autowired AutoPlayScheduler autoPlayScheduler;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired InstalledCardRepository installedCardRepository;
    @Autowired AcquiredCardRepository acquiredCardRepository;
    @Autowired LeadingPlayerRepository leadingPlayerRepository;
    @Autowired InFlightManager inFlightManager;
    @Autowired RoomCleanupService roomCleanupService;
    @Autowired WsRoomHandler wsRoomHandler;

    private long roomId;

    @AfterEach
    void cleanup() {
        roomCleanupService.cleanupRoomData(roomId).block();
    }

    @Test
    @DisplayName("같은 턴에 대한 동시 카드 제출은 정확히 한 번만 실행된다 (@GameLock + fresh 재검증)")
    void duplicateSubmitExecutesExactlyOnce() throws Exception {
        roomId = 930_001L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();
        installedCardRepository.savePlayerCards(List.of(Card.JAN_3), roomId, Player.PLAYER_1).block();
        installedCardRepository.saveHiddenCard(List.of(Card.FEB_3), roomId).block();

        // 유저 요청과 자동플레이가 같은 낡은 상태로 동시에 진입한 상황
        RaceResult result = race(2, () ->
                turnFlowService.processNormalSubmit(roomId, Player.PLAYER_1, 0, null, autoPlayScheduler).block());

        assertEquals(1, result.successes(), "카드 제출은 정확히 한 번만 실행돼야 한다");
        for (Throwable failure : result.failures()) {
            assertInstanceOf(WebSocketBusinessException.class, failure,
                    "패자는 비즈니스 에러(TRY_AGAIN/NOT_YOUR_TURN 등)로 거절돼야 한다: " + failure);
        }

        GameState after = awaitState(gs -> gs.getCurrentTurn() == 2, 3000);
        assertEquals(2, after.getCurrentTurn(), "턴은 정확히 한 번만 전환돼야 한다");
        assertEquals(GamePhase.IN_PROGRESS, after.getPhase());
        assertTrue(installedCardRepository.getPlayerCards(roomId, Player.PLAYER_1).block().isEmpty(),
                "손패는 한 장만 소비돼야 한다");
    }

    @Test
    @DisplayName("동시 GO 선언은 정확히 한 번만 반영된다")
    void duplicateGoAppliesExactlyOnce() throws Exception {
        roomId = 930_002L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.AWAITING_GO_STOP_CHOICE)
                .player1(PlayerState.builder().score(7).build())
                .build();
        gameStateRepository.create(state).block();

        RaceResult result = race(2, () ->
                turnFlowService.processGoStopChoice(roomId, Player.PLAYER_1, true, null, autoPlayScheduler).block());

        assertEquals(1, result.successes(), "GO 선언은 정확히 한 번만 실행돼야 한다");

        GameState after = gameStateRepository.findById(roomId).block();
        assertEquals(GamePhase.IN_PROGRESS, after.getPhase());
        assertEquals(2, after.getCurrentTurn());
        assertEquals(1, after.getPlayerState(Player.PLAYER_1).getGo(), "고 횟수가 중복 반영되면 안 된다");
    }

    @Test
    @DisplayName("자동플레이가 유저 요청에 양보한 뒤, 유저가 행동을 끝내면 재시도가 중복 실행하지 않는다")
    void yieldedAutoPlayDoesNotDoubleExecuteAfterUserAction() throws Exception {
        roomId = 930_003L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                .choiceInfo(ChoiceInfo.builder()
                        .playerNumToChoose(Player.PLAYER_1)
                        .submittedCard(Card.JAN_3)
                        .selectableCards(List.of(Card.JAN_1, Card.JAN_2))
                        .build())
                .build();
        gameStateRepository.create(state).block();
        installedCardRepository.saveRevealedCard(List.of(Card.JAN_1, Card.JAN_2), roomId).block();

        // 유저 요청 진행 중(NORMAL 플래그 on) 상태에서 자동플레이 타이머 발사 → 양보 후 1초 주기 재시도
        String normalKey = InFlightManager.normalKey(roomId, 1);
        String token = "user-request";
        inFlightManager.trySetFlag(normalKey, token, Duration.ofSeconds(30)).block();
        autoPlayScheduler.scheduleAutoPlay(roomId, 1, 1, Player.PLAYER_1, System.nanoTime(), GamePhase.AWAITING_FLOOR_CARD_CHOICE);
        Thread.sleep(400);

        // 유저가 자동플레이(0번: JAN_1)와 다른 1번 선택지(JAN_2)로 행동을 완료
        turnFlowService.processFloorSelection(roomId, Player.PLAYER_1, 1, null, autoPlayScheduler).block();
        inFlightManager.deleteFlag(normalKey, token).block();

        // 재시도 주기(1초)가 지나도 자동 선택이 중복 실행되지 않아야 한다
        Thread.sleep(2000);

        GameState after = gameStateRepository.findById(roomId).block();
        assertEquals(2, after.getCurrentTurn(), "턴은 유저 행동으로 한 번만 전환돼야 한다");
        List<Card> acquired = acquiredCardRepository.getAllCards(roomId, 1).block();
        assertTrue(acquired.containsAll(List.of(Card.JAN_2, Card.JAN_3)), "유저 선택 결과여야 한다: " + acquired);
        assertFalse(acquired.contains(Card.JAN_1), "양보했던 자동플레이의 0번 선택이 중복 실행되면 안 된다: " + acquired);
    }

    @Test
    @DisplayName("동시 READY로 둘 다 준비돼도 게임 시작은 한 번만 발화한다 (방 단위 락)")
    void concurrentReadyStartsGameExactlyOnce() throws Exception {
        roomId = 930_004L;
        GameState state = GameState.createEmptyRoom(roomId).join(101L).join(202L);
        gameStateRepository.create(state).block();

        RequestEvent<Void> readyEvent = new RequestEvent<>();
        readyEvent.setSubCategory(SubCategory.READY);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        try {
            for (Player player : List.of(Player.PLAYER_1, Player.PLAYER_2)) {
                pool.submit(() -> {
                    try {
                        start.await();
                        wsRoomHandler.handleRoomEvent(readyEvent, state, player).block();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable t) {
                        failures.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertTrue(failures.isEmpty(), "READY는 직렬화될 뿐 실패하면 안 된다: " + failures);
        GameState after = gameStateRepository.findById(roomId).block();
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, after.getPhase());
        assertEquals(5, leadingPlayerRepository.getAllCards(roomId).block().size(),
                "선 선택 카드 5장이 두 번 뽑히면(10장) 시작이 중복 발화된 것이다");

        // 시작 후의 READY는 거절된다 — 통과시키면 GameState 전체 덮어쓰기가 @GameLock의 턴 전이를 되돌린다
        WebSocketBusinessException rejected = assertThrows(WebSocketBusinessException.class,
                () -> wsRoomHandler.handleRoomEvent(readyEvent, state, Player.PLAYER_1).block());
        assertEquals(INVALID_GAME_PHASE, rejected.getWebsocketErrorCode());
        assertEquals(5, leadingPlayerRepository.getAllCards(roomId).block().size());
        assertEquals(GamePhase.DETERMINING_STARTING_PLAYER, gameStateRepository.findById(roomId).block().getPhase());
    }

    @Test
    @DisplayName("진행 중 READY는 거절돼 턴 상태를 덮어쓰지 않는다")
    void readyDuringGameDoesNotClobberTurnState() {
        roomId = 930_008L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(2).round(4)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();

        RequestEvent<Void> readyEvent = new RequestEvent<>();
        readyEvent.setSubCategory(SubCategory.READY);

        // 핸들러가 받는 건 낡은 스냅샷일 수 있으므로 방 락 안 fresh 상태로 걸러져야 한다
        GameState stale = GameState.createEmptyRoom(roomId);
        WebSocketBusinessException rejected = assertThrows(WebSocketBusinessException.class,
                () -> wsRoomHandler.handleRoomEvent(readyEvent, stale, Player.PLAYER_1).block());
        assertEquals(INVALID_GAME_PHASE, rejected.getWebsocketErrorCode());

        GameState after = gameStateRepository.findById(roomId).block();
        assertEquals(GamePhase.IN_PROGRESS, after.getPhase());
        assertEquals(4, after.getRound(), "READY가 라운드를 되돌리면 안 된다");
        assertEquals(2, after.getCurrentTurn(), "READY가 턴을 되돌리면 안 된다");
    }

    @Test
    @DisplayName("카드 제출의 턴 전환은 락 안에서 저장된다 (후처리 이전에 이미 다음 턴)")
    void submitPersistsTurnTransitionInsideLock() {
        roomId = 930_005L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();
        installedCardRepository.savePlayerCards(List.of(Card.JAN_3), roomId, Player.PLAYER_1).block();
        installedCardRepository.saveHiddenCard(List.of(Card.FEB_3), roomId).block();

        // TurnFlowService 후처리 없이 락 구간만 실행 — 락 해제 직후의 저장 상태를 검증
        gamePlayService.executeNormalSubmit(roomId, Player.PLAYER_1, 0, null).block();

        GameState after = gameStateRepository.findById(roomId).block();
        assertEquals(2, after.getCurrentTurn(), "턴 전환이 락 안에서 저장돼야 한다");
        assertEquals(GamePhase.IN_PROGRESS, after.getPhase());
    }

    @Test
    @DisplayName("점수 달성 제출의 고/스톱 대기 진입은 락 안에서 저장된다")
    void submitPersistsGoStopEntryInsideLock() {
        roomId = 930_006L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();
        // 광 5장 = 15점 → 제출 턴 완료 시 canGoStop
        acquiredCardRepository.addCards(roomId, 1, List.of(Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.NOV_1, Card.DEC_1)).block();
        installedCardRepository.savePlayerCards(List.of(Card.FEB_1), roomId, Player.PLAYER_1).block();
        installedCardRepository.saveHiddenCard(List.of(Card.MAR_3), roomId).block();

        gamePlayService.executeNormalSubmit(roomId, Player.PLAYER_1, 0, null).block();

        GameState after = gameStateRepository.findById(roomId).block();
        assertEquals(GamePhase.AWAITING_GO_STOP_CHOICE, after.getPhase(), "고/스톱 대기 진입이 락 안에서 저장돼야 한다");
        assertEquals(1, after.getCurrentTurn(), "고/스톱 대기 중엔 턴이 유지돼야 한다");
    }

    @Test
    @DisplayName("바닥 선택 완료의 턴 전환도 락 안에서 저장된다")
    void floorSelectionPersistsTurnTransitionInsideLock() {
        roomId = 930_007L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                .choiceInfo(ChoiceInfo.builder()
                        .playerNumToChoose(Player.PLAYER_1)
                        .submittedCard(Card.JAN_3)
                        .selectableCards(List.of(Card.JAN_1, Card.JAN_2))
                        .build())
                .build();
        gameStateRepository.create(state).block();
        installedCardRepository.saveRevealedCard(List.of(Card.JAN_1, Card.JAN_2), roomId).block();

        gamePlayService.executeFloorSelection(roomId, Player.PLAYER_1, 0, null).block();

        GameState after = gameStateRepository.findById(roomId).block();
        assertEquals(2, after.getCurrentTurn(), "턴 전환이 락 안에서 저장돼야 한다");
        assertEquals(GamePhase.IN_PROGRESS, after.getPhase());
        assertNull(after.getChoiceInfo());
    }

    @Test
    @DisplayName("락 해제~후처리(브로드캐스트) 사이 창구에 도착한 낡은 제출은 거절된다")
    void lateSubmitInPostLockWindowIsRejected() {
        roomId = 930_008L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
        gameStateRepository.create(state).block();
        installedCardRepository.savePlayerCards(List.of(Card.JAN_3, Card.FEB_1), roomId, Player.PLAYER_1).block();
        installedCardRepository.saveHiddenCard(List.of(Card.FEB_3), roomId).block();

        // 자동플레이의 락 구간이 끝났지만 후처리(브로드캐스트/타이머)는 아직인 창구를 재현:
        // 락 구간만 실행하고 후처리를 생략한 채 낡은 상태의 유저 제출을 밀어넣는다
        gamePlayService.executeNormalSubmit(roomId, Player.PLAYER_1, 0, null).block();

        WebSocketBusinessException e = assertThrows(WebSocketBusinessException.class,
                () -> turnFlowService.processNormalSubmit(roomId, Player.PLAYER_1, 0, null, autoPlayScheduler).block());
        assertEquals(NOT_YOUR_TURN, e.getWebsocketErrorCode());

        assertEquals(List.of(Card.FEB_1), installedCardRepository.getPlayerCards(roomId, Player.PLAYER_1).block(),
                "한 턴에 카드가 두 장 나가면 안 된다");
        assertEquals(2, gameStateRepository.findById(roomId).block().getCurrentTurn());
    }

    @Test
    @DisplayName("STOP은 락 안에서 END를 저장하고, 창구에 도착한 낡은 GO는 거절된다")
    void stopPersistsEndInsideLockAndRejectsLateGo() {
        roomId = 930_009L;
        GameState state = GameState.builder()
                .roomId(roomId).leadingPlayer(1).currentTurn(1).round(1)
                .phase(GamePhase.AWAITING_GO_STOP_CHOICE)
                .player1(PlayerState.builder().score(7).build())
                .build();
        gameStateRepository.create(state).block();

        // STOP의 락 구간만 실행 (게임 종료 정리는 아직) — 이 시점에 이미 END가 저장돼 있어야 한다
        gamePlayService.executeGoStop(roomId, Player.PLAYER_1, false, null).block();
        assertEquals(GamePhase.END, gameStateRepository.findById(roomId).block().getPhase(),
                "STOP의 END 전이가 락 안에서 저장돼야 한다");

        WebSocketBusinessException e = assertThrows(WebSocketBusinessException.class,
                () -> turnFlowService.processGoStopChoice(roomId, Player.PLAYER_1, true, null, autoPlayScheduler).block());
        assertEquals(INVALID_GAME_PHASE, e.getWebsocketErrorCode());
        assertEquals(0, gameStateRepository.findById(roomId).block().getPlayerState(Player.PLAYER_1).getGo(),
                "STOP 뒤에 낡은 GO가 반영되면 안 된다");
    }

    private record RaceResult(int successes, List<Throwable> failures) {}

    /** 같은 액션을 n개 스레드가 동시에 시도하고 성공/실패를 집계한다 */
    private RaceResult race(int threads, Runnable action) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger successes = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        action.run();
                        successes.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable t) {
                        failures.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS), "동시 실행이 제한 시간 내에 끝나야 한다");
        } finally {
            pool.shutdownNow();
        }
        return new RaceResult(successes.get(), List.copyOf(failures));
    }

    private GameState awaitState(Predicate<GameState> condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        GameState last = null;
        while (System.nanoTime() < deadline) {
            last = gameStateRepository.findById(roomId).block();
            if (last != null && condition.test(last)) {
                return last;
            }
            Thread.sleep(50);
        }
        return last;
    }
}
