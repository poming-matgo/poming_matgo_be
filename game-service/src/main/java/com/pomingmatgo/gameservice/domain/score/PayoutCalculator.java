package com.pomingmatgo.gameservice.domain.score;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/** 카드 점수에 고 보너스와 배수 룰을 적용해 정산 점수를 낸다 — 배수 룰 자체는 Multiplier가 소유한다 */
@Component
public class PayoutCalculator {

    /** 승패가 갈린 최종 정산 (박 계열 포함). 무승부는 정산 없음 */
    public Payout finalPayout(GameState gameState, Player winner) {
        if (winner == Player.PLAYER_NOTHING) {
            return Payout.NONE;
        }
        return calculate(contextOf(gameState, winner), scope -> true);
    }

    /** 진행 중 표시용 — 승패에 따라 결정되는 VERSUS 배수는 아직 확정할 수 없어 제외한다 */
    public Payout provisionalPayout(GameState gameState, Player player) {
        return calculate(contextOf(gameState, player), scope -> scope == Multiplier.Scope.SELF);
    }

    private PayoutContext contextOf(GameState gameState, Player winner) {
        return PayoutContext.of(gameState.getPlayerState(winner), gameState.getPlayerState(winner.opponent()));
    }

    private Payout calculate(PayoutContext ctx, Predicate<Multiplier.Scope> scopeFilter) {
        int baseScore = ctx.winner().getScore() + ctx.winner().getGo();

        List<Payout.Applied> applied = new ArrayList<>();
        int total = baseScore;
        for (Multiplier multiplier : Multiplier.values()) {
            if (!scopeFilter.test(multiplier.getScope()) || !multiplier.appliesTo(ctx)) {
                continue;
            }
            int factor = multiplier.factor(ctx);
            applied.add(new Payout.Applied(multiplier, multiplier.getDisplayName(), factor));
            total *= factor;
        }
        return new Payout(baseScore, List.copyOf(applied), total);
    }
}
