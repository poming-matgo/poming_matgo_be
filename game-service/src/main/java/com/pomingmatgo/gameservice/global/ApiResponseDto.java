package com.pomingmatgo.gameservice.global;

import lombok.Getter;

@Getter
public class ApiResponseDto<T> {
    private final int status;
    private final String message;
    private T data;

    public ApiResponseDto(int status, String message) {
        this.status = status;
        this.message = message;
    }

    public ApiResponseDto(int status, String message, T data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }

}
