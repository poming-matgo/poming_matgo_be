package com.pomingmatgo.gameservice.domain.score;

import java.util.List;

/** baseScore = 카드 점수 + 고 보너스, total = baseScore에 적용된 배수를 모두 곱한 값 */
public record Payout(int baseScore, List<Applied> multipliers, int total) {

    public record Applied(Multiplier type, String displayName, int factor) {}

    public static final Payout NONE = new Payout(0, List.of(), 0);

    public boolean has(Multiplier multiplier) {
        return multipliers.stream().anyMatch(applied -> applied.type() == multiplier);
    }
}
