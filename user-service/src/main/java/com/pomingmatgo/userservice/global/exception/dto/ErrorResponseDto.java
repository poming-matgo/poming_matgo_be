package com.pomingmatgo.userservice.global.exception.dto;

import com.pomingmatgo.userservice.global.exception.ErrorCode;
import lombok.Getter;

@Getter
public class ErrorResponseDto {
    private final String errorCode;
    private final int httpStatus;
    private final String errorMessage;

    public ErrorResponseDto(ErrorCode errorCode) {
        this.errorCode = errorCode.name();
        this.httpStatus = errorCode.getStatusCode();
        this.errorMessage = errorCode.getMessage();
    }
}