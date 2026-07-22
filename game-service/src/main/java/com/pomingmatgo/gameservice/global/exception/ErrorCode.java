package com.pomingmatgo.gameservice.global.exception;

import lombok.Getter;

@Getter
public enum ErrorCode {
    //비즈니스 예외

    //방 입장
    ALREADY_EXISTED_ROOM(409, "이미 존재하는 방입니다."),
    NOT_EXISTED_ROOM(404, "존재하지 않는 방입니다."),
    FULL_ROOM(409, "방이 꽉 찼습니다."),
    ALREADY_IN_ROOM(409, "이미 방에 입장했습니다."),
    GAME_IN_PROGRESS(409, "게임이 진행 중인 방입니다."),

    //시스템 예외
    SYSTEM_ERROR(500, "시스템 에러가 발생했습니다. 관리자에게 문의하세요.");


    private final int statusCode;
    private final String message;

    ErrorCode(int status, String message) {
        this.statusCode = status;
        this.message = message;
    }

}
