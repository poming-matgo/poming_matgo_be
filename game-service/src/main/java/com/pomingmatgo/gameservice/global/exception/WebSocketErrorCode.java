package com.pomingmatgo.gameservice.global.exception;

import lombok.Getter;

@Getter
public enum WebSocketErrorCode {


    //유저
    INVALID_USER("유효하지 않은 사용자입니다."),
    ALREADY_JOIN("이미 다른방에 접속해있습니다."),
    //비즈니스 에러
    NOT_EXISTED_ROOM( "존재하지 않는 방입니다."),
    NOT_IN_ROOM("방에 입장하지 않았습니다."),

    //선두 플레이어 선택
    ALREADY_SELECTED_CARD("이미 선택된 카드입니다."),

    SYSTEM_ERROR("시스템 에러가 발생했습니다. 관리자에게 문의하세요."),

    //카드 제출
    INVALID_CARD("유효하지 않은 카드입니다."),
    NOT_EXIST_FLOOR_CARD("선택할 수 있는 바닥패가 없습니다."),
    NOT_YOUR_TURN("턴이 아닙니다.");

    private final String message;

    WebSocketErrorCode(String message) {
        this.message = message;
    }
}
