package com.pomingmatgo.gameservice.domain.score;

import com.pomingmatgo.gameservice.domain.PlayerState;

// 배수 룰 판정 입력. 진행 중 표시(provisional)에서는 winner가 "이겼다고 가정한 플레이어"이며 VERSUS 룰은 평가되지 않는다
public record PayoutContext(PlayerState winner, PlayerState loser,
                            ScoreBreakdown winnerBreakdown, ScoreBreakdown loserBreakdown) {

    public static PayoutContext of(PlayerState winner, PlayerState loser) {
        return new PayoutContext(winner, loser, breakdownOf(winner), breakdownOf(loser));
    }

    // 첫 턴 이전엔 점수 계산이 돌지 않아 breakdown이 없다
    private static ScoreBreakdown breakdownOf(PlayerState state) {
        return state.getBreakdown() != null ? state.getBreakdown() : ScoreBreakdown.empty();
    }
}
