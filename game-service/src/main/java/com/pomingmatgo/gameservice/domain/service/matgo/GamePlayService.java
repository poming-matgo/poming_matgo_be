package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.NOT_YOUR_TURN;

@Service
@RequiredArgsConstructor
public class GamePlayService {
    private final GameService gameService;

    @GameLock(key = "'game:' + #roomId")
    public Mono<TurnExecutionResult> executeNormalSubmit(long roomId, GameState gameState, Player player, int cardIdx, Runnable onLockAcquired) {
        return validatedFreshState(roomId, GamePhase.IN_PROGRESS, player, onLockAcquired)
                .flatMap(freshState ->
                        Mono.zip(gameService.takeCardFromHand(roomId, player, cardIdx), gameService.drawTopCard(roomId))
                                .flatMap(tuple -> {
                                    Card submittedCard = tuple.getT1();
                                    Card topCard = tuple.getT2();
                                    return gameService.submitCard(freshState, submittedCard, topCard)
                                            .flatMap(processResult -> settleTurn(roomId, freshState, processResult)
                                                    .map(nextState -> new TurnExecutionResult(
                                                            submittedCard, topCard, processResult, nextState)));
                                }));
    }

    @GameLock(key = "'game:' + #roomId")
    public Mono<FloorSelectionResult> executeFloorSelection(long roomId, GameState gameState, Player player, int cardIdx, Runnable onLockAcquired) {
        return validatedFreshState(roomId, GamePhase.AWAITING_FLOOR_CARD_CHOICE, player, onLockAcquired)
                .flatMap(freshState -> gameService.selectFloorCard(freshState, player, cardIdx)
                        .flatMap(result -> settleTurn(roomId, freshState, result)
                                .map(nextState -> new FloorSelectionResult(result, nextState))));
    }

    /**
     * @GameLock 획득 직후의 공통 전처리: 자동플레이 타이머 취소 콜백 실행 → fresh 상태 재조회 →
     * phase/차례 재검증. 락 통과 후에도 자동플레이와의 race로 상태가 이미 진행됐을 수 있어
     * 세 게임 액션(제출/바닥 선택/고스톱) 모두 fresh 상태 기준으로 검증한다.
     */
    private Mono<GameState> validatedFreshState(long roomId, GamePhase expectedPhase, Player player, Runnable onLockAcquired) {
        return Mono.defer(() -> {
            if (onLockAcquired != null) {
                onLockAcquired.run();
            }
            return gameService.findGameState(roomId);
        }).flatMap(freshState -> {
            if (freshState.getPhase() != expectedPhase) {
                return Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE));
            }
            if (!player.equals(freshState.getCurrentPlayer())) {
                return Mono.error(new WebSocketBusinessException(NOT_YOUR_TURN));
            }
            return Mono.just(freshState);
        });
    }

    /**
     * 턴 확정 공통 후처리: 피 뺏기/획득 반영 후 점수를 재계산하고, 다음 상태
     * (고스톱 대기/게임 종료/다음 턴)까지 결정해 저장한 상태를 반환한다.
     * 다음 상태 저장은 반드시 @GameLock 안에서 끝나야 한다 — 락 해제 후로 미루면
     * 그 사이(결과 브로드캐스트 등) 낡은 경쟁자가 phase/차례 재검증을 통과해 같은 턴을 중복 실행한다.
     * 선택 대기(choiceRequired)면 아직 턴이 끝나지 않았으므로 아무것도 반영하지 않고 기존 상태 그대로 반환.
     */
    private Mono<GameState> settleTurn(long roomId, GameState gameState, ProcessCardResult result) {
        if (result.isChoiceRequired()) {
            return Mono.just(gameState);
        }
        Mono<Void> loseCards = Flux.fromIterable(result.getMoveCards())
                .concatMap(card -> gameService.loseCard(roomId, gameState.getOtherPlayer(), card))
                .then();

        return loseCards
                .then(gameService.acquireCards(roomId, gameState.getCurrentPlayer(), result.getAcquiredCards()))
                .then(gameService.calculateAndApplyScores(roomId, gameState))
                .flatMap(this::transitionAfterTurn);
    }

    /** 턴 완료 후 다음 상태 결정+저장: 최종 라운드 점수 달성/마지막 턴 미달성 → 종료, 점수 달성 → 고/스톱 대기, 그 외 → 다음 턴 */
    private Mono<GameState> transitionAfterTurn(GameState settled) {
        Player player = settled.getCurrentPlayer();
        if (settled.canGoStop(player)) {
            // 마지막 라운드엔 GO 선택지가 없으므로 자동 스톱으로 곧바로 게임 종료
            return settled.isFinalRound()
                    ? markEnded(settled)
                    : gameService.enterGoStopChoice(settled);
        }
        // 상대는 직전 턴(자동 스톱 판정)에서 이미 미달이었고 이번 턴에 점수가 오를 수 없으므로 둘 다 스톱 불가 → 무승부
        return settled.isLastTurn() ? markEnded(settled) : proceedToNextTurn(settled);
    }

    private Mono<GameState> markEnded(GameState gameState) {
        return gameService.saveState(gameState.toBuilder().phase(GamePhase.END).build());
    }

    private Mono<GameState> proceedToNextTurn(GameState gameState) {
        return gameService.saveState(gameState.setNextTurn());
    }

    @GameLock(key = "'game:' + #roomId")
    public Mono<GameState> executeGoStop(long roomId, GameState gameState, Player player, boolean go, Runnable onLockAcquired) {
        return validatedFreshState(roomId, GamePhase.AWAITING_GO_STOP_CHOICE, player, onLockAcquired)
                .flatMap(freshState -> go
                        ? gameService.applyGo(freshState, player).flatMap(this::proceedToNextTurn)
                        // STOP도 락 안에서 END를 저장 — 저장 없이 반환하면 락 해제~cleanup 사이 낡은 GO가 재검증을 통과한다
                        : markEnded(freshState));
    }

    public Mono<GameState> gameOver(GameState gameState) {
        return gameService.gameOver(gameState);
    }
}
