package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.messaging.ResponseEvent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum SpecialEvent {
    NONE("없음", null), // NONE은 전송 대상이 아님 (GameMessageSender에서 필터)
    PPEOK("뻑!", ResponseEvent.PPEOK),
    TTADAK("따닥!", ResponseEvent.TTADAK),
    JJOK("쪽!", ResponseEvent.JJOK),
    // 카드 매칭 결과가 아니라 뻑 누적 판정 결과라 CardMatchEngine이 아닌 TurnFlowService가 발행한다
    THREE_PPEOK("세번뻑!", ResponseEvent.THREE_PPEOK);

    private final String displayName;
    private final ResponseEvent responseEvent;
}
