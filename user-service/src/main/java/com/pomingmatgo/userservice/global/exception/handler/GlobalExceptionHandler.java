package com.pomingmatgo.userservice.global.exception.handler;

import com.pomingmatgo.userservice.global.exception.ErrorCode;
import com.pomingmatgo.userservice.global.exception.dto.ErrorResponseDto;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BindException.class)
    public ErrorResponseDto handleBindException(BindException e) {
        ErrorCode errorCode = ErrorCode.INVALID_FORM_INPUT;
        return new ErrorResponseDto(errorCode.name(), errorCode.getStatusCode(), errorCode.getMessage());
    }
}
