package com.pomingmatgo.userservice.global.exception;

import lombok.Getter;

@Getter
public class SystemException extends RuntimeException {
    private final ErrorCode errorCode;

    public SystemException(ErrorCode errorCode) {
        this.errorCode = errorCode;
    }
}

