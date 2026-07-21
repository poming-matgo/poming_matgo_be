package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.messaging.ReconnectStateRes;
import com.pomingmatgo.gameservice.domain.messaging.ScoreInfoRes;
import com.pomingmatgo.gameservice.domain.ChoiceInfo;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.scheduler.TurnScheduler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.NOT_EXISTED_ROOM;

/**
 * 재접속한 플레이어에게 보낼 게임 상태 스냅샷을 조립한다.
 *
 * 상태는 호출 시점에 fresh 조회한다 — 이탈 중 자동플레이가 게임을 진행시켰을 수 있고,
 * 이탈자 전원 퇴장으로 방이 정리(teardown)됐다면 NOT_EXISTED_ROOM으로 실패해
 * 재접속 자체를 거절한다.
 *
 * 스냅샷 조립은 락 없이 여러 repository를 읽으므로 자동플레이 진행과 겹치면 필드 간
 * 미세한 어긋남이 있을 수 있다 — 이후 브로드캐스트 메시지가 도착하며 수렴하므로 허용한다.
 */
@Service
@RequiredArgsConstructor
public class ReconnectService {

    private final GameStateRepository gameStateRepository;
    private final InstalledCardRepository installedCardRepository;
    private final AcquiredCardRepository acquiredCardRepository;
    private final TurnScheduler turnScheduler;

    public Mono<ReconnectStateRes> buildSnapshot(long roomId, Player me) {
        Player opponent = me.opponent();

        return gameStateRepository.findById(roomId)
                .switchIfEmpty(Mono.error(new WebSocketBusinessException(NOT_EXISTED_ROOM)))
                .flatMap(gameState -> Mono.zip(
                        installedCardRepository.getPlayerCards(roomId, me),
                        installedCardRepository.getPlayerCards(roomId, opponent),
                        installedCardRepository.getAllRevealedCards(roomId),
                        acquiredCardRepository.getAllCards(roomId, me.getNumber()),
                        acquiredCardRepository.getAllCards(roomId, opponent.getNumber())
                ).map(t -> assemble(gameState, me, opponent,
                        t.getT1(), t.getT2().size(), t.getT3(), t.getT4(), t.getT5())));
    }

    private ReconnectStateRes assemble(GameState gameState, Player me, Player opponent,
                                       List<Card> myCards, int opponentCardCount, List<Card> floorCards,
                                       List<Card> myAcquired, List<Card> opponentAcquired) {
        return ReconnectStateRes.builder()
                .you(me)
                .round(gameState.getRound())
                .currentTurn(gameState.getCurrentTurn())
                .currentPlayer(gameState.getCurrentPlayer())
                .phase(gameState.getPhase())
                .leadingPlayer(gameState.getLeadingPlayer())
                .remainingMs(turnScheduler.getRemainingTurnMillis(gameState.getRoomId()))
                .myCards(myCards)
                .opponentCardCount(opponentCardCount)
                .floorCards(floorCards.stream().collect(Collectors.groupingBy(Card::getMonth)))
                .myAcquiredCards(myAcquired.stream().collect(Collectors.groupingBy(Card::getType)))
                .opponentAcquiredCards(opponentAcquired.stream().collect(Collectors.groupingBy(Card::getType)))
                .scores(ScoreInfoRes.from(gameState).getScores())
                .myGo(gameState.getPlayerState(me).getGo())
                .opponentGo(gameState.getPlayerState(opponent).getGo())
                .selectableCards(pendingFloorChoice(gameState, me))
                .nextGoNum(pendingGoStopChoice(gameState, me))
                .build();
    }

    private List<Card> pendingFloorChoice(GameState gameState, Player me) {
        if (gameState.getPhase() != GamePhase.AWAITING_FLOOR_CARD_CHOICE) return null;
        ChoiceInfo choiceInfo = gameState.getChoiceInfo();
        if (choiceInfo == null || choiceInfo.getPlayerNumToChoose() != me) return null;
        return choiceInfo.getSelectableCards();
    }

    private Integer pendingGoStopChoice(GameState gameState, Player me) {
        if (gameState.getPhase() != GamePhase.AWAITING_GO_STOP_CHOICE) return null;
        if (gameState.getCurrentPlayer() != me) return null;
        return gameState.getPlayerState(me).getGo() + 1;
    }
}
