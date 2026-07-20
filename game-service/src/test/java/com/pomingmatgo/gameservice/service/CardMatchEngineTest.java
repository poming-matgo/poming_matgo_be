package com.pomingmatgo.gameservice.service;

import com.pomingmatgo.gameservice.domain.ChoiceInfo;
import com.pomingmatgo.gameservice.domain.service.matgo.CardMatchEngine;
import com.pomingmatgo.gameservice.domain.service.matgo.CardMatchEngine.FloorEffect;
import com.pomingmatgo.gameservice.domain.service.matgo.CardMatchEngine.MatchOutcome;
import com.pomingmatgo.gameservice.domain.service.matgo.SpecialEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_1;
import static com.pomingmatgo.gameservice.domain.card.Card.*;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("카드 매칭 규칙 엔진")
class CardMatchEngineTest {

    private final CardMatchEngine engine = new CardMatchEngine();

    @Test
    @DisplayName("같은 월 + 바닥 1장이면 뻑 — 획득 없이 세 장이 바닥에 쌓인다")
    void ppeok() {
        MatchOutcome outcome = engine.decideSubmit(PLAYER_1, JAN_1, JAN_2,
                List.of(JAN_3), List.of(), List.of());

        assertThat(outcome.result().getSpecialEvents()).containsExactly(SpecialEvent.PPEOK);
        assertThat(outcome.result().getAcquiredCards()).isEmpty();
        assertThat(outcome.effects()).containsExactly(new FloorEffect.Place(List.of(JAN_2, JAN_1)));
    }

    @Test
    @DisplayName("같은 월 + 빈 바닥이면 쪽 — 상대 피를 한 장 가져온다")
    void jjok() {
        MatchOutcome outcome = engine.decideSubmit(PLAYER_1, JAN_1, JAN_2,
                List.of(), List.of(), List.of(FEB_3));

        assertThat(outcome.result().getSpecialEvents()).containsExactly(SpecialEvent.JJOK);
        assertThat(outcome.result().getAcquiredCards()).containsExactlyInAnyOrder(JAN_1, JAN_2, FEB_3);
        assertThat(outcome.result().getMoveCards()).containsExactly(FEB_3);
    }

    @Test
    @DisplayName("따닥이라도 상대에게 피가 없으면 피 뺏기 없는 일반 획득이 된다")
    void ttadakWithoutOpponentPi() {
        MatchOutcome outcome = engine.decideSubmit(PLAYER_1, JAN_1, JAN_2,
                List.of(JAN_3, JAN_4), List.of(), List.of(FEB_1, MAR_2));

        assertThat(outcome.result().getSpecialEvents()).isEmpty();
        assertThat(outcome.result().getMoveCards()).isEmpty();
        assertThat(outcome.result().getAcquiredCards()).containsExactlyInAnyOrder(JAN_1, JAN_2, JAN_3, JAN_4);
    }

    @Test
    @DisplayName("피를 뺏을 땐 쌍피보다 일반 피를 먼저 가져온다")
    void preferNormalPiOverSsangPi() {
        MatchOutcome outcome = engine.decideSubmit(PLAYER_1, JAN_1, JAN_2,
                List.of(), List.of(), List.of(NOV_4, FEB_3));

        assertThat(outcome.result().getMoveCards()).containsExactly(FEB_3);
    }

    @Test
    @DisplayName("낸 카드와 뒤집은 카드가 모두 3장 스택을 쓸면 서로 다른 피를 뺏는다")
    void doubleStealTakesDistinctPi() {
        MatchOutcome outcome = engine.decideSubmit(PLAYER_1, JAN_1, FEB_1,
                List.of(JAN_2, JAN_3, JAN_4), List.of(FEB_2, FEB_3, FEB_4), List.of(MAR_3, MAR_4));

        assertThat(outcome.result().getMoveCards()).containsExactlyInAnyOrder(MAR_3, MAR_4);
    }

    @Test
    @DisplayName("뒤집은 카드가 2장 스택을 만나면 앞선 획득분이 pendingChoice.prevCards로 이월된다")
    void carryoverIntoPendingChoice() {
        MatchOutcome outcome = engine.decideSubmit(PLAYER_1, JAN_1, FEB_1,
                List.of(JAN_2), List.of(FEB_2, FEB_3), List.of());

        assertThat(outcome.result().isChoiceRequired()).isTrue();
        ChoiceInfo pending = outcome.pendingChoice();
        assertThat(pending.getSubmittedCard()).isEqualTo(FEB_1);
        assertThat(pending.getTurnedCard()).isNull();
        assertThat(pending.getPrevCards()).containsExactlyInAnyOrder(JAN_1, JAN_2);
        // 낸 카드의 바닥 정리는 선택 대기와 무관하게 적용돼야 한다
        assertThat(outcome.effects()).containsExactly(new FloorEffect.ClearMonth(1));
    }

    @Test
    @DisplayName("바닥 선택 완료 시 이월분+선택 획득이 최종 결과로 합쳐지고 선택 카드만 바닥에서 제거된다")
    void floorSelectionMergesCarryover() {
        ChoiceInfo choiceInfo = ChoiceInfo.builder()
                .playerNumToChoose(PLAYER_1)
                .submittedCard(FEB_1)
                .selectableCards(List.of(FEB_2, FEB_3))
                .turnedCard(null)
                .prevCards(List.of(JAN_1, JAN_2, MAR_3))
                .prevMoveCards(List.of(MAR_3))
                .build();

        MatchOutcome outcome = engine.decideFloorSelection(PLAYER_1, choiceInfo, FEB_2, List.of(), List.of());

        assertThat(outcome.result().getAcquiredCards())
                .containsExactlyInAnyOrder(JAN_1, JAN_2, MAR_3, FEB_1, FEB_2);
        assertThat(outcome.result().getMoveCards()).containsExactly(MAR_3);
        assertThat(outcome.effects()).containsExactly(new FloorEffect.Remove(FEB_2));
    }
}
