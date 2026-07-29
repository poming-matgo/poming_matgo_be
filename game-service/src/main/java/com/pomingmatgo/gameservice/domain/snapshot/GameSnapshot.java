package com.pomingmatgo.gameservice.domain.snapshot;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.card.Card;

import java.util.List;

/**
 * seq N 커맨드 직후의 전 저장소 상태 — 복구 = 이 스냅샷 로드 + seq N+1부터 replay.
 * LeadingPlayerRepository는 pre-game 전용이라 라운드 경계 시점엔 복구 대상이 아니다
 */
public record GameSnapshot(
        long roomId,
        long seq,
        GameState gameState,   // toBuilder 기반 불변 객체라 참조 보관으로 충분
        List<Card> p1Hand,
        List<Card> p2Hand,
        List<Card> floorCards,
        List<Card> hiddenDeck, // draw 순서 그대로
        List<Card> p1Acquired,
        List<Card> p2Acquired
) {
    public GameSnapshot {
        p1Hand = List.copyOf(p1Hand);
        p2Hand = List.copyOf(p2Hand);
        floorCards = List.copyOf(floorCards);
        hiddenDeck = List.copyOf(hiddenDeck);
        p1Acquired = List.copyOf(p1Acquired);
        p2Acquired = List.copyOf(p2Acquired);
    }
}
