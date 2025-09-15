package com.pomingmatgo.gameservice.domain.card;

import lombok.Getter;

@Getter
public enum SpecialType {
    HONG_DAN(CardType.DDI),
    CHUNG_DAN(CardType.DDI),
    CHO_DAN(CardType.DDI),

    BI_GWANG(CardType.GWANG),

    GODORI(CardType.KKUT);

    private final CardType validType;

    private SpecialType(CardType validType) {
        this.validType = validType;
    }
}
