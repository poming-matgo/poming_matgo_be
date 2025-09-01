package com.pomingmatgo.userservice.global.exception.dto;

import lombok.Getter;

@Getter
public class ErrorResponseDto {
    private final String errorCode;
    private final int httpStatus;
    private final String errorMessage;

    public ErrorResponseDto(String errorCode, int httpStatus, String errorMessage) {
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
        this.errorMessage = errorMessage;
    }
}