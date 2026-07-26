package com.pomingmatgo.gameservice.domain.messaging;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.card.CardType;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

// 각 필드 포맷은 대응하는 개별 메시지(DISTRIBUTED_FLOOR_CARD/ACQUIRED_CARD/SCORE_UPDATE/GO_STOP_CHOICE)와 같다.
// 상대 손패는 장수만 노출한다
@Getter
@Builder
public class ReconnectStateRes {
    private final Player you;
    private final int round;
    private final int currentTurn;
    private final Player currentPlayer;
    private final GamePhase phase;
    private final int leadingPlayer;
    private final long remainingMs;

    private final List<Card> myCards;
    private final int opponentCardCount;
    private final Map<Integer, List<Card>> floorCards;
    private final Map<CardType, List<Card>> myAcquiredCards;
    private final Map<CardType, List<Card>> opponentAcquiredCards;
    private final List<PlayerScoreDto> scores;
    private final int myGo;
    private final int opponentGo;

    /** AWAITING_FLOOR_CARD_CHOICE이고 내 선택 차례일 때만 채워진다 */
    private final List<Card> selectableCards;
    /** AWAITING_GO_STOP_CHOICE이고 내 선택 차례일 때만 채워진다 */
    private final GoStopChoiceRes goStopChoice;
}
