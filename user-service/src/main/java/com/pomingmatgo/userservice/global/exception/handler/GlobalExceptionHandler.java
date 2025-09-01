package com.pomingmatgo.userservice.global.exception.handler;

import com.pomingmatgo.userservice.global.exception.BusinessException;
import com.pomingmatgo.userservice.global.exception.ErrorCode;
import com.pomingmatgo.userservice.global.exception.SystemException;
import com.pomingmatgo.userservice.global.exception.dto.ErrorResponseDto;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BindException.class)
    public ErrorResponseDto handleBindException(BindException e) {
        return new ErrorResponseDto(ErrorCode.INVALID_FORM_INPUT);
    }

    @ExceptionHandler(BusinessException.class)
    public ErrorResponseDto handleBusinessException(BusinessException e) {
        return new ErrorResponseDto(e.getErrorCode());
    }

    @ExceptionHandler(SystemException.class)
    public ErrorResponseDto handleBusinessException(SystemException e) {
        return new ErrorResponseDto(e.getErrorCode());
    }
}
