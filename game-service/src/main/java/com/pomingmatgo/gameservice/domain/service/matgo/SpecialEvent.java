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
    JJOK("쪽!", ResponseEvent.JJOK);

    private final String displayName;
    private final ResponseEvent responseEvent;
}
