package com.pomingmatgo.gameservice.global;

import lombok.Getter;

@Getter
public class ApiResponseDto<T> {
    private final int status;
    private final String message;

    public ApiResponseDto(int status, String message) {
        this.status = status;
        this.message = message;
    }
}
