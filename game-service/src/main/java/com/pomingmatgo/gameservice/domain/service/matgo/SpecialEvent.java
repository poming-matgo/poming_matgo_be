package com.pomingmatgo.gameservice.domain.service.matgo;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum SpecialEvent {
    NONE("없음"),
    PPEOK("뻑!"),
    TTADAK("따닥!"),
    JJOK("쪽!");

    private final String displayName;
}