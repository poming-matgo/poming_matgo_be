package com.pomingmatgo.userservice.global.exception;

import lombok.Getter;

@Getter
public enum ErrorCode {
    //비즈니스 예외
    //입력 값 에러
    INVALID_FORM_INPUT(400, "폼의 입력 데이터가 유효하지 않습니다."),
    DUPLICATE_EMAIL(407, "이메일이 중복됩니다."),
    DUPLICATE_NICKNAME(407, "닉네임이 중복됩니다."),


    //시스템 예외
    //범용
    SYSTEM_ERROR(500, "시스템 에러가 발생했습니다. 관리자에게 문의하세요."),
    //이메일
    EMAIL_SEND_FAILED(502, "이메일 서버와의 연결중 에러가 발생했습니다.");


    private final int statusCode;
    private final String message;

    ErrorCode(int status, String message) {
        this.statusCode = status;
        this.message = message;
    }

}
