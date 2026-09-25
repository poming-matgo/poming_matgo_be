package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InMemoryInstalledCardRepository;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TRY_AGAIN;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doAnswer;

// 현재 동작의 결함 재현이다. 실행 소유권 도입 시 정리 대기·낡은 실행 거부 기대값으로 전환한다.
@SpringBootTest(properties = "spring.autoconfigure.exclude="
        + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration")
@ActiveProfiles("in-memory")
@DisplayName("게임 액션과 방 정리·재생성 경합 기준선")
class GameActionCleanupRaceTest {
    private static final long ROOM_ID = 940_002L;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    @Autowired GamePlayService gamePlayService;
    @Autowired GameStateRepository gameStateRepository;
    @Autowired AcquiredCardRepository acquiredCardRepository;
    @Autowired RoomCleanupService roomCleanupService;
    @SpyBean InMemoryInstalledCardRepository installedCardRepository;

    private final Sinks.Empty<Void> gate = Sinks.empty();
    private final AtomicInteger waiting = new AtomicInteger();
    private final AtomicInteger succeeded = new AtomicInteger();

    @BeforeEach
    void setUp() {
        assertTrue(AopUtils.isAopProxy(gamePlayService));
        gameStateRepository.create(state(1, GamePhase.IN_PROGRESS)).block(TIMEOUT);
        installedCardRepository.savePlayerCards(List.of(Card.JAN_3), ROOM_ID, Player.PLAYER_1).block(TIMEOUT);
        installedCardRepository.savePlayerCards(List.of(Card.FEB_3), ROOM_ID, Player.PLAYER_2).block(TIMEOUT);
        installedCardRepository.saveHiddenCard(List.of(Card.MAR_1, Card.APR_1), ROOM_ID).block(TIMEOUT);
        pauseAfterDraw();
    }

    @AfterEach
    void tearDown() {
        roomCleanupService.cleanupRoom(ROOM_ID).block(TIMEOUT);
    }

    @ParameterizedTest(name = "전체 정리={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("정리는 진행 중 액션을 기다리지 않고, 늦은 액션은 삭제된 방의 바닥 카드를 다시 만든다")
    void lateActionRecreatesCardsAfterCleanup(boolean fullCleanup) {
        StepVerifier.create(submit())
                .then(() -> {
                    assertPausedAndLocked();
                    cleanup(fullCleanup).block(TIMEOUT);
                    assertRoomAbsent();
                    assertEquals(0, succeeded.get());
                    assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
                })
                .expectErrorSatisfies(error -> assertEquals(ErrorCode.SYSTEM_ERROR,
                        assertInstanceOf(BusinessException.class, error).getErrorCode()))
                .verify(TIMEOUT);

        assertNull(gameStateRepository.findById(ROOM_ID).block(TIMEOUT), "상태 save는 삭제된 방을 복원하지 않는다");
        assertEquals(List.of(Card.JAN_3, Card.MAR_1), floor(), "상태 저장 실패 전에 카드 쓰기는 이미 발생한다");
        assertEquals(0, succeeded.get());
    }

    @ParameterizedTest(name = "전체 정리={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("같은 ID 재생성은 새 락으로 진입하고, 이전 액션이 새 방의 종료 상태를 덮는다")
    void oldActionOverwritesRecreatedRoom(boolean fullCleanup) {
        StepVerifier.create(submit())
                .then(() -> {
                    assertPausedAndLocked();
                    cleanup(fullCleanup).block(TIMEOUT);
                    assertRoomAbsent();
                    gameStateRepository.create(state(7, GamePhase.AWAITING_GO_STOP_CHOICE)).block(TIMEOUT);
                    GameState ended = gamePlayService.executeGoStop(ROOM_ID, Player.PLAYER_1, false, null)
                            .block(TIMEOUT);
                    assertNotNull(ended);
                    assertEquals(GamePhase.END, ended.getPhase(), "이전 액션이 락 안에 있어도 새 액션이 완료된다");
                    assertEquals(7, ended.getRound());
                    assertEquals(0, succeeded.get());
                    assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
                })
                .assertNext(result -> assertEquals(2, result.updatedGameState().getCurrentTurn()))
                .expectComplete().verify(TIMEOUT);

        GameState overwritten = gameStateRepository.findById(ROOM_ID).block(TIMEOUT);
        assertNotNull(overwritten);
        assertEquals(1, overwritten.getRound());
        assertEquals(GamePhase.IN_PROGRESS, overwritten.getPhase());
        assertEquals(Player.PLAYER_2, overwritten.getCurrentPlayer());
        assertEquals(List.of(Card.JAN_3, Card.MAR_1), floor());
        assertEquals(1, succeeded.get());
    }

    @ParameterizedTest(name = "전체 정리={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("대조군: 액션 완료 후 정리하면 상태와 카드가 남지 않는다")
    void cleanupAfterActionCompletionLeavesNoCards(boolean fullCleanup) {
        StepVerifier.create(submit())
                .then(() -> {
                    assertPausedAndLocked();
                    assertEquals(Sinks.EmitResult.OK, gate.tryEmitEmpty());
                })
                .assertNext(result -> assertEquals(2, result.updatedGameState().getCurrentTurn()))
                .expectComplete().verify(TIMEOUT);
        assertEquals(1, succeeded.get());
        assertEquals(List.of(Card.JAN_3, Card.MAR_1), floor());
        cleanup(fullCleanup).block(TIMEOUT);
        assertRoomAbsent();
    }

    @SuppressWarnings("unchecked")
    private void pauseAfterDraw() {
        doAnswer(invocation -> {
            Mono<Card> draw = (Mono<Card>) invocation.callRealMethod();
            return draw.delayUntil(card -> Mono.defer(() -> {
                waiting.incrementAndGet();
                return gate.asMono();
            }));
        }).when(installedCardRepository).drawTopCard(ROOM_ID);
    }

    private GameState state(int round, GamePhase phase) {
        return GameState.builder().roomId(ROOM_ID).leadingPlayer(1).currentTurn(1)
                .round(round).phase(phase).build();
    }

    private Mono<TurnExecutionResult> submit() {
        return gamePlayService.executeNormalSubmit(ROOM_ID, Player.PLAYER_1, 0, succeeded::incrementAndGet);
    }

    private Mono<Void> cleanup(boolean fullCleanup) {
        return fullCleanup ? roomCleanupService.cleanupRoom(ROOM_ID) : roomCleanupService.cleanupRoomData(ROOM_ID);
    }

    private void assertPausedAndLocked() {
        assertEquals(1, waiting.get());
        assertEquals(0, succeeded.get());
        StepVerifier.create(submit())
                .expectErrorSatisfies(error -> assertEquals(TRY_AGAIN,
                        assertInstanceOf(WebSocketBusinessException.class, error).getWebsocketErrorCode()))
                .verify(TIMEOUT);
    }

    private List<Card> floor() {
        return installedCardRepository.getAllRevealedCards(ROOM_ID).block(TIMEOUT);
    }

    private void assertRoomAbsent() {
        assertNull(gameStateRepository.findById(ROOM_ID).block(TIMEOUT));
        assertEquals(List.of(), floor());
        assertNull(installedCardRepository.drawTopCard(ROOM_ID).block(TIMEOUT));
        for (Player player : List.of(Player.PLAYER_1, Player.PLAYER_2)) {
            assertEquals(List.of(), installedCardRepository.getPlayerCards(ROOM_ID, player).block(TIMEOUT));
            assertEquals(List.of(), acquiredCardRepository.getAllCards(ROOM_ID, player.getNumber()).block(TIMEOUT));
        }
    }
}
