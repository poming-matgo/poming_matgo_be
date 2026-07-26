package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.domain.card.Card;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChoiceInfo implements Serializable {
    private Player playerNumToChoose;
    private Card submittedCard;
    private List<Card> selectableCards;
    private Card turnedCard;
    // 이 턴에서 이미 확정된 획득/피 뺏기 이월분 — 없으면 선택 대기 순간 앞선 획득 카드가 유실된다
    private List<Card> prevCards;
    private List<Card> prevMoveCards;
}
