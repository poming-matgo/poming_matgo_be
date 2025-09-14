package com.pomingmatgo.gameservice.global.exception;

import lombok.Getter;

@Getter
public enum WebSocketErrorCode {


    //비즈니스 에러
    NOT_EXISTED_ROOM( "존재하지 않는 방입니다."),

    SYSTEM_ERROR("시스템 에러가 발생했습니다. 관리자에게 문의하세요.");


    private final String message;

    WebSocketErrorCode(String message) {
        this.message = message;
    }
}
