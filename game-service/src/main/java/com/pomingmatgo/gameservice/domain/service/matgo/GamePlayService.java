package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandLog;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.snapshot.GameSnapshotService;
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
    private final GameCommandLog gameCommandLog;
    private final GameSnapshotService gameSnapshotService;

    @GameLock
    public Mono<TurnExecutionResult> executeNormalSubmit(long roomId, Player player, int cardIdx, Runnable onLockAcquired) {
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
                                }))
                // 상태 저장 성공 후, 아직 @GameLock 안 — seq 부여가 락 밖이면 append 순서 역전이 가능하다
                .delayUntil(result -> logThenSnapshot(roomId, GameCommandType.NORMAL_SUBMIT, player, cardIdx, false,
                        GamePhase.IN_PROGRESS, result.cardResult(), result.updatedGameState()));
    }

    @GameLock
    public Mono<FloorSelectionResult> executeFloorSelection(long roomId, Player player, int cardIdx, Runnable onLockAcquired) {
        return validatedFreshState(roomId, GamePhase.AWAITING_FLOOR_CARD_CHOICE, player, onLockAcquired)
                .flatMap(freshState -> gameService.selectFloorCard(freshState, player, cardIdx)
                        .flatMap(result -> settleTurn(roomId, freshState, result)
                                .map(nextState -> new FloorSelectionResult(result, nextState))))
                .delayUntil(result -> logThenSnapshot(roomId, GameCommandType.FLOOR_SELECT, player, cardIdx, false,
                        GamePhase.AWAITING_FLOOR_CARD_CHOICE, result.cardResult(), result.updatedGameState()));
    }

    // 선택 대기로 끝난 턴은 settleTurn이 상태 객체를 갱신하지 않으므로 저장된 phase는 AWAITING_FLOOR_CARD_CHOICE다
    private GamePhase resultingPhase(ProcessCardResult result, GameState nextState) {
        return result.isChoiceRequired() ? GamePhase.AWAITING_FLOOR_CARD_CHOICE : nextState.getPhase();
    }

    // 라운드 경계면 로그와 같은 seq로 스냅샷 캡처 — 아직 락 안이라 4개 저장소가 같은 시점이다.
    // 경계 판정도 저장된 phase(resultingPhase) 기준 — 선택 대기 턴의 stale nextState 오판 방지
    private Mono<Void> logThenSnapshot(long roomId, GameCommandType type, Player player, int cardIdx, boolean go,
                                       GamePhase prevPhase, ProcessCardResult cardResult, GameState nextState) {
        GamePhase nextPhase = resultingPhase(cardResult, nextState);
        return gameCommandLog.logCommand(roomId, type, player, cardIdx, go, prevPhase, nextPhase)
                .flatMap(seq -> gameSnapshotService.captureIfRoundStart(roomId, seq, nextPhase, nextState));
    }

    // 락 통과 후에도 자동플레이 race로 상태가 이미 진행됐을 수 있어 fresh 상태로 재검증한다
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

    // 다음 상태 저장까지 @GameLock 안에서 끝내야 한다 — 락 해제 후로 미루면
    // 그 사이 낡은 경쟁자가 phase/차례 재검증을 통과해 같은 턴을 중복 실행한다
    private Mono<GameState> settleTurn(long roomId, GameState gameState, ProcessCardResult result) {
        if (result.isChoiceRequired()) {
            return Mono.just(gameState);
        }
        Mono<Void> loseCards = Flux.fromIterable(result.getMoveCards())
                .concatMap(card -> gameService.loseCard(roomId, gameState.getOtherPlayer(), card))
                .then();

        return loseCards
                .then(gameService.acquireCards(roomId, gameState.getCurrentPlayer(), result.getAcquiredCards()))
                .then(gameService.calculateAndApplyScores(roomId, countPpeok(gameState, result)))
                .flatMap(this::transitionAfterTurn);
    }

    // 뻑 누적은 점수 저장(calculateAndApplyScores)에 실려 함께 영속된다
    private GameState countPpeok(GameState gameState, ProcessCardResult result) {
        if (!result.getSpecialEvents().contains(SpecialEvent.PPEOK)) {
            return gameState;
        }
        return gameState.updatePlayerState(gameState.getCurrentPlayer(),
                ps -> ps.toBuilder().ppeokCount(ps.getPpeokCount() + 1).build());
    }

    private Mono<GameState> transitionAfterTurn(GameState settled) {
        Player player = settled.getCurrentPlayer();
        // 세번뻑은 점수와 무관한 즉시 승리라 고/스톱 판정보다 앞선다
        if (settled.hasPpeokWin(player)) {
            return markEnded(settled);
        }
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

    @GameLock
    public Mono<GameState> executeGoStop(long roomId, Player player, boolean go, Runnable onLockAcquired) {
        return validatedFreshState(roomId, GamePhase.AWAITING_GO_STOP_CHOICE, player, onLockAcquired)
                .flatMap(freshState -> go
                        ? gameService.applyGo(freshState, player).flatMap(this::proceedToNextTurn)
                        // STOP도 락 안에서 END를 저장 — 저장 없이 반환하면 락 해제~cleanup 사이 낡은 GO가 재검증을 통과한다
                        : markEnded(freshState))
                .delayUntil(nextState -> gameCommandLog.logCommand(roomId, GameCommandType.GO_STOP, player, 0,
                                go, GamePhase.AWAITING_GO_STOP_CHOICE, nextState.getPhase())
                        .flatMap(seq -> gameSnapshotService.captureIfRoundStart(roomId, seq, nextState.getPhase(), nextState)));
    }

    public Mono<GameState> gameOver(GameState gameState) {
        return gameService.gameOver(gameState);
    }
}
