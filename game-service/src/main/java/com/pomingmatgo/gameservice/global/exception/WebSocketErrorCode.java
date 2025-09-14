package com.pomingmatgo.gameservice.global.exception;

import lombok.Getter;

@Getter
public enum WebSocketErrorCode {

    SYSTEM_ERROR("시스템 에러가 발생했습니다. 관리자에게 문의하세요.");

    private final String message;

    WebSocketErrorCode(String message) {
        this.message = message;
    }
}
