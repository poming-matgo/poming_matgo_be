package com.pomingmatgo.gameservice.domain.score;

import lombok.Getter;

/**
 * 정산 배수 룰 — 새 배수(피박/광박/멍박/흔들기/폭탄)는 상수 한 블록 추가로 편입된다.
 * PayoutCalculator는 values()를 순회할 뿐 개별 룰을 알지 않는다.
 */
@Getter
public enum Multiplier {

    GO_MULTIPLIER("고 배수", Scope.SELF) {
        @Override
        public boolean appliesTo(PayoutContext ctx) {
            return ctx.winner().getGo() >= GO_MULTIPLIER_FROM;
        }

        @Override
        public int factor(PayoutContext ctx) {
            return 1 << (ctx.winner().getGo() - GO_MULTIPLIER_FROM + 1);
        }
    },

    GO_BAK("고박", Scope.VERSUS) {
        @Override
        public boolean appliesTo(PayoutContext ctx) {
            return ctx.loser().getGo() > 0;
        }
    };

    // 3고부터 고 1회당 2배
    static final int GO_MULTIPLIER_FROM = 3;

    /** SELF는 승자 자신의 상태만으로, VERSUS는 승패가 갈려야 결정된다 (진행 중 표시에선 VERSUS 제외) */
    public enum Scope { SELF, VERSUS }

    private final String displayName;
    private final Scope scope;

    Multiplier(String displayName, Scope scope) {
        this.displayName = displayName;
        this.scope = scope;
    }

    public abstract boolean appliesTo(PayoutContext ctx);

    public int factor(PayoutContext ctx) {
        return 2;
    }
}
