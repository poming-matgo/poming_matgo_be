package com.pomingmatgo.gameservice.application.pregame;

import com.pomingmatgo.gameservice.application.game.TurnFlowService;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.messaging.LeadSelectionRes;
import com.pomingmatgo.gameservice.infrastructure.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.infrastructure.scheduler.TurnScheduler;
import com.pomingmatgo.gameservice.infrastructure.scheduler.RoomTimerLifecycle;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_1;
import static com.pomingmatgo.gameservice.domain.Player.PLAYER_2;
import static com.pomingmatgo.gameservice.domain.Player.PLAYER_NOTHING;
import static com.pomingmatgo.gameservice.domain.TurnTiming.TURN_TIMEOUT_MILLIS;

// TurnScheduler를 빈으로 주입해도 되는 이유: AutoPlayScheduler가 이 서비스를 의존하지 않아 DI cycle이 없다
@Service
@RequiredArgsConstructor
public class PreGameFlowService {

    private final RoomTimerLifecycle timerLifecycle;
    private final PreGameService preGameService;
    private final GameMessageSender gameMessageSender;
    private final TurnFlowService turnFlowService;
    private final TurnScheduler turnScheduler;

    public Mono<Void> processLeaderSelection(GameState gameState, Player player, int cardIndex) {
        return Mono.defer(() -> processLeaderSelectionInRoom(gameState, player, cardIndex,
                timerLifecycle.bind(gameState.getRoomId(), turnScheduler)));
    }

    private Mono<Void> processLeaderSelectionInRoom(GameState gameState, Player player, int cardIndex, TurnScheduler scheduler) {
        long roomId = gameState.getRoomId();

        return preGameService.selectLeaderCard(roomId, player, cardIndex)
                .then(preGameService.checkAllSelected(roomId))
                .flatMap(allSelected -> {
                    Mono<Void> selected = Mono.defer(() ->
                            gameMessageSender.sendLeaderSelectionMessage(roomId, player, cardIndex));
                    return allSelected
                            ? prepareGameStart(gameState, scheduler)
                                    .flatMap(start -> selected.then(notifyGameStart(roomId, start)))
                            : selected;
                });
    }

    // 필수 저장·타이머 또는 재시작을 먼저 끝내고, 안내는 연결 구독에서 순서대로 전송한다.
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

    private Mono<Void> notifyGameStart(long roomId, PreparedStart start) {
        return Mono.defer(() -> gameMessageSender.sendLeaderSelectionResult(roomId, start.result()))
                .then(Mono.defer(() -> gameMessageSender.sendDistributedCardInfo(roomId, start.cards())))
                .then(Mono.defer(() -> start.draw()
                        ? turnFlowService.announceGameOver(start.state(), PLAYER_NOTHING)
                        : gameMessageSender.sendTurnInfo(start.state(), TURN_TIMEOUT_MILLIS)));
    }

    private record PreparedStart(LeadSelectionRes result, InstalledCard cards, GameState state, boolean draw) {}

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
