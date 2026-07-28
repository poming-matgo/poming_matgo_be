package com.pomingmatgo.gameservice.domain.gamelog;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;

import java.util.List;

/** 레코드 1건 = @GameLock 임계 구역 1회. seq는 방 로컬 단조 증가, 첫 레코드는 항상 DECK_INIT */
public record GameLogRecord(
        long roomId,
        long seq,
        GameCommandType type,
        Player player,      // DECK_INIT이면 null
        int cardIndex,      // NORMAL_SUBMIT/FLOOR_SELECT에서만 의미
        boolean go,         // GO_STOP에서만 의미
        List<Card> deck,    // DECK_INIT에서만 — 셔플 확정 덱 전체 (replay의 유일한 비결정 입력)
        GamePhase prevPhase,
        GamePhase nextPhase
) {
    public static GameLogRecord deckInit(long roomId, long seq, List<Card> deck) {
        return new GameLogRecord(roomId, seq, GameCommandType.DECK_INIT, null, 0, false, List.copyOf(deck), null, null);
    }

    public static GameLogRecord command(long roomId, long seq, GameCommandType type, Player player,
                                        int cardIndex, boolean go, GamePhase prevPhase, GamePhase nextPhase) {
        return new GameLogRecord(roomId, seq, type, player, cardIndex, go, null, prevPhase, nextPhase);
    }
}
