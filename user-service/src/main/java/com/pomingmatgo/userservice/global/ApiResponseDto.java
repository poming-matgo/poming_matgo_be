package com.pomingmatgo.userservice.global;

import lombok.Getter;

@Getter
public class ApiResponseDto<T> {
    private int status;
    private String message;
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
