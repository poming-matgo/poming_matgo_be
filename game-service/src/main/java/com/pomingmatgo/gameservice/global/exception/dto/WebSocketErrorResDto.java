package com.pomingmatgo.gameservice.global.exception.dto;

import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import lombok.Getter;

@Getter
public class WebSocketErrorResDto {
    private final String errorCode;
    private final String errorMessage;

    public WebSocketErrorResDto(WebSocketErrorCode errorCode) {
        this.errorCode = errorCode.name();
        this.errorMessage = errorCode.getMessage();
    }
}
