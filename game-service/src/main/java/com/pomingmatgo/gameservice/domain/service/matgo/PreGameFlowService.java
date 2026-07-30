package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.lease.RoomLeaseManager;
import com.pomingmatgo.gameservice.domain.messaging.GameMessageSender;
import com.pomingmatgo.gameservice.scheduler.TurnScheduler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_1;
import static com.pomingmatgo.gameservice.domain.Player.PLAYER_2;
import static com.pomingmatgo.gameservice.domain.Player.PLAYER_NOTHING;

// TurnScheduler를 빈으로 주입해도 되는 이유: AutoPlayScheduler가 이 서비스를 의존하지 않아 DI cycle이 없다
@Service
@RequiredArgsConstructor
public class PreGameFlowService {

    private final PreGameService preGameService;
    private final GameMessageSender gameMessageSender;
    private final TurnFlowService turnFlowService;
    private final TurnScheduler turnScheduler;
    private final RoomLeaseManager roomLeaseManager;

    public Mono<Void> processLeaderSelection(GameState gameState, Player player, int cardIndex) {
        long roomId = gameState.getRoomId();

        return preGameService.selectLeaderCard(roomId, player, cardIndex)
                .then(preGameService.checkAllSelected(roomId))
                .flatMap(allSelected -> gameMessageSender.sendLeaderSelectionMessage(roomId, player, cardIndex)
                        .then(allSelected
                                ? proceedToGameStart(gameState)
                                : Mono.empty()));
    }

    private Mono<Void> proceedToGameStart(GameState gameState) {
        // durable 기록(DECK_INIT)이 시작되기 전에 소유권(lease)부터 확보한다 — 실패는 게임 시작 중단(fail-fast)
        return roomLeaseManager.acquire(gameState.getRoomId())
                .then(finalizeLeaderSelection(gameState))
                .flatMap(this::distributeCardsAndNotify)
                .flatMap(this::checkChongtongAndProceed)
                .flatMap(this::startFirstTurn);
    }

    private Mono<GameState> finalizeLeaderSelection(GameState gameState) {
        long roomId = gameState.getRoomId();
        return preGameService.getLeadSelectionRes(roomId)
                .flatMap(leadSelectionRes -> {
                    GameState updatedState = gameState.toBuilder()
                            .leadingPlayer(leadSelectionRes.getLeadPlayer())
                            .build();

                    return gameMessageSender.sendLeaderSelectionResult(roomId, leadSelectionRes)
                            .thenReturn(updatedState);
                });
    }

    private Mono<GameState> distributeCardsAndNotify(GameState gameState) {
        long roomId = gameState.getRoomId();
        return Mono.defer(() -> preGameService.distributeCards(roomId))
                .delayUntil(cards -> gameMessageSender.sendDistributedCardInfo(roomId, cards))
                .flatMap(cards -> checkFloorDrawAndProceed(gameState, cards));
    }

    // 첫 턴 시작 전에 끝나는 무승부 — empty를 반환해 이후 시작 단계를 건너뛴다
    private Mono<GameState> checkFloorDrawAndProceed(GameState gameState, InstalledCard cards) {
        if (!cards.hasFourOfSameMonthOnFloor()) {
            return Mono.just(gameState);
        }
        return turnFlowService.processGameOver(gameState, PLAYER_NOTHING).then(Mono.empty());
    }

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

    private Mono<Void> startFirstTurn(GameState gameState) {
        return preGameService.setFirstTurn(gameState)
                .flatMap(state -> turnFlowService.startTurn(state, turnScheduler));
    }
}
