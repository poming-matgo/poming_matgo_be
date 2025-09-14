package com.pomingmatgo.gameservice.global.exception;

import lombok.Getter;

@Getter
public class WebSocketBusinessException extends RuntimeException {
    private final WebSocketErrorCode websocketErrorCode;

    public WebSocketBusinessException(WebSocketErrorCode websocketErrorCode) {
        this.websocketErrorCode = websocketErrorCode;
    }
}