package com.pomingmatgo.gameservice.application.pregame;

import com.pomingmatgo.gameservice.application.game.TurnFlowService;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.infrastructure.lock.GameLock;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.messaging.LeadSelectionRes;
import com.pomingmatgo.gameservice.infrastructure.scheduler.TurnScheduler;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_1;
import static com.pomingmatgo.gameservice.domain.Player.PLAYER_2;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;

@Service
@RequiredArgsConstructor
public class PreGameStartService {
    private final GameStateRepository states;
    private final PreGameService preGameService;
    private final TurnFlowService turnFlowService;
    private final RoomTimerLifecycle timerLifecycle;
    private final TurnScheduler turnScheduler;

    // 선택 검증 뒤 첫 저장 전 수락한다. 트리거·분배·첫 턴 또는 무승부 재시작까지 같은 gate를 유지한다.
    @GameLock
    public Mono<PreparedStart> selectAndPrepare(long roomId, Player player, int cardIndex, Runnable onPrepared) {
        return states.findById(roomId)
                .filter(state -> state.getPhase() == GamePhase.DETERMINING_STARTING_PLAYER)
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE)))
                .flatMap(state -> preGameService.selectLeaderCard(roomId, player, cardIndex)
                        .then(Mono.defer(() -> preGameService.checkAllSelected(roomId)
                                .flatMap(selected -> selected
                                        ? preGameService.tryClaimLeaderSelectionTrigger(roomId)
                                        : Mono.just(false))
                                .flatMap(ready -> ready
                                        ? prepareGameStart(state, timerLifecycle.bind(roomId, turnScheduler))
                                        : Mono.empty()))))
                // 첫 선택의 빈 결과도 저장 완료이며, 오류·취소 신호에서는 예약하지 않는다.
                .doOnSuccess(start -> onPrepared.run());
    }

    private Mono<PreparedStart> prepareGameStart(GameState gameState, TurnScheduler scheduler) {
        long roomId = gameState.getRoomId();
        return preGameService.getLeadSelectionRes(roomId)
                .flatMap(result -> {
                    GameState updatedState = gameState.toBuilder()
                            .leadingPlayer(result.getLeadPlayer())
                            .build();
                    return Mono.defer(() -> preGameService.distributeCards(roomId))
                            .flatMap(cards -> {
                                boolean draw = cards.hasFourOfSameMonthOnFloor();
                                Mono<GameState> completion = draw
                                        ? turnFlowService.completeGameOver(updatedState)
                                        : checkChongtongAndProceed(updatedState)
                                                .flatMap(preGameService::setFirstTurn)
                                                .flatMap(state -> turnFlowService.prepareFirstTurn(state, scheduler));
                                return completion.map(state -> new PreparedStart(result, cards, state, draw));
                            });
                });
    }

    public record PreparedStart(LeadSelectionRes result, InstalledCard cards, GameState state, boolean draw) {}

    // todo: 승부판정 로직 구현 필요
    private Mono<GameState> checkChongtongAndProceed(GameState gameState) {
        long roomId = gameState.getRoomId();

        Mono<Boolean> p1HasChongtong = preGameService.hasChongtong(roomId, PLAYER_1)
                .defaultIfEmpty(false);
        Mono<Boolean> p2HasChongtong = preGameService.hasChongtong(roomId, PLAYER_2)
                .defaultIfEmpty(false);

        return Mono.zip(p1HasChongtong, p2HasChongtong)
                .flatMap(tuple -> {
                    boolean p1Result = tuple.getT1();
                    boolean p2Result = tuple.getT2();

                    if (p1Result && p2Result) {
                        // 둘 다 총통인 경우 — todo: 무승부 처리
                        return Mono.just(gameState);
                    }
                    if (p1Result || p2Result) {
                        // todo: 총통 승부 처리
                        return Mono.just(gameState);
                    }
                    return Mono.just(gameState);
                });
    }

}
