package com.pomingmatgo.gameservice.global.exception;

import lombok.Getter;

@Getter
public enum WebSocketErrorCode {


    //비즈니스 에러
    NOT_EXISTED_ROOM( "존재하지 않는 방입니다."),
    NOT_IN_ROOM("방에 입장하지 않았습니다."),

    //선두 플레이어 선택
    ALREADY_SELECTED_CARD("이미 선택된 카드입니다."),

    SYSTEM_ERROR("시스템 에러가 발생했습니다. 관리자에게 문의하세요."),

    //카드 제출
    INVALUD_CARD("유효하지 않은 카드입니다.");

    private final String message;

    WebSocketErrorCode(String message) {
        this.message = message;
    }
}
