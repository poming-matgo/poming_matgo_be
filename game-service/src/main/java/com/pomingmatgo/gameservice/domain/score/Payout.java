package com.pomingmatgo.gameservice.domain.score;

import java.util.List;

/** baseScore = 카드 점수 + 고 보너스, total = baseScore에 적용된 배수를 모두 곱한 값 */
public record Payout(int baseScore, List<Applied> multipliers, int total) {

    public record Applied(Multiplier type, String displayName, int factor) {}

    public static final Payout NONE = new Payout(0, List.of(), 0);

    /** 고 보너스도 배수도 붙지 않는 고정 점수 정산 (세번뻑 등 즉시 승리) */
    public static Payout flat(int score) {
        return new Payout(score, List.of(), score);
    }

    public boolean has(Multiplier multiplier) {
        return multipliers.stream().anyMatch(applied -> applied.type() == multiplier);
    }
}
