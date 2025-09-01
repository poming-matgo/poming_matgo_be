package com.pomingmatgo.userservice.global.exception.dto;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class ErrorResponseDto<T> {
    private final int statusCode;
    private final String httpStatus;
    private final T errorMessage;

    public ErrorResponseDto(int statusCode, HttpStatus httpStatus, T errorMessage) {
        this.statusCode = statusCode;
        this.httpStatus = httpStatus.getReasonPhrase();
        this.errorMessage = errorMessage;
    }
}