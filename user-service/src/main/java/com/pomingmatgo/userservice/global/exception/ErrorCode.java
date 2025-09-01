package com.pomingmatgo.userservice.global.exception;

import lombok.Getter;

@Getter
public enum ErrorCode {
    //입력 값 에러
    INVALID_FORM_INPUT(400, "폼의 입력 데이터가 유효하지 않습니다."),
    DUPLICATE_EMAIL(407, "이메일이 중복됩니다."),
    DUPLICATE_NICKNAME(400, "닉네임이 중복됩니다.");

    private final int statusCode;
    private final String message;

    ErrorCode(int status, String message) {
        this.statusCode = status;
        this.message = message;
    }

}
