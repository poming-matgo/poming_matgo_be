package com.pomingmatgo.gameservice.domain.card;

import lombok.Getter;

@Getter
public enum Card {
    JAN_1(1, CardType.GWANG),
    JAN_2(1, CardType.DDI, SpecialType.HONG_DAN),
    JAN_3(1, CardType.PI),
    JAN_4(1, CardType.PI),
    FEB_1(2, CardType.KKUT, SpecialType.GODORI),
    FEB_2(2, CardType.DDI, SpecialType.HONG_DAN),
    FEB_3(2, CardType.PI),
    FEB_4(2, CardType.PI),
    MAR_1(3, CardType.GWANG),
    MAR_2(3, CardType.DDI, SpecialType.HONG_DAN),
    MAR_3(3, CardType.PI),
    MAR_4(3, CardType.PI),
    APR_1(4, CardType.KKUT, SpecialType.GODORI),
    APR_2(4, CardType.DDI, SpecialType.CHO_DAN),
    APR_3(4, CardType.PI),
    APR_4(4, CardType.PI),
    MAY_1(5, CardType.KKUT),
    MAY_2(5, CardType.DDI, SpecialType.CHO_DAN),
    MAY_3(5, CardType.PI),
    MAY_4(5, CardType.PI),
    JUN_1(6, CardType.KKUT),
    JUN_2(6, CardType.DDI, SpecialType.CHUNG_DAN),
    JUN_3(6, CardType.PI),
    JUN_4(6, CardType.PI),
    JUL_1(7, CardType.KKUT),
    JUL_2(7, CardType.DDI, SpecialType.CHO_DAN),
    JUL_3(7, CardType.PI),
    JUL_4(7, CardType.PI),
    AUG_1(8, CardType.GWANG),
    AUG_2(8, CardType.KKUT, SpecialType.GODORI),
    AUG_3(8, CardType.PI),
    AUG_4(8, CardType.PI),
    SEP_1(9, CardType.DDI, SpecialType.CHUNG_DAN),
    SEP_2(9, CardType.PI),
    SEP_3(9, CardType.PI),
    SEP_4(9, CardType.KKUT), //todo: 쌍피변환 구현 해야함
    OCT_1(10, CardType.KKUT),
    OCT_2(10, CardType.DDI, SpecialType.CHUNG_DAN),
    OCT_3(10, CardType.PI),
    OCT_4(10, CardType.PI),
    NOV_1(11, CardType.GWANG),
    NOV_2(11, CardType.PI),
    NOV_3(11, CardType.PI),
    NOV_4(11, CardType.PI, SpecialType.SSANG_PI),
    DEC_1(12, CardType.GWANG, SpecialType.BI_GWANG),
    DEC_2(12, CardType.KKUT),
    DEC_3(12, CardType.DDI),
    DEC_4(12, CardType.PI, SpecialType.SSANG_PI);


    private final int month;
    private final CardType type;
    private final SpecialType specialType;

    Card(int month, CardType type, SpecialType specialType) {
        this.month = month;
        this.type = type;
        this.specialType = specialType;
    }

    Card(int month, CardType type) {
        this.month = month;
        this.type = type;
        this.specialType = null;
    }

    public boolean hasSameMonthAs(Card other) {
        return this.getMonth() == other.getMonth();
    }
}