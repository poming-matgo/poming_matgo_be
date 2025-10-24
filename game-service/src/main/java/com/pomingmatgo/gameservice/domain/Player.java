package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.Getter;

import java.util.Arrays;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_USER;

@Getter
public enum Player {
    PLAYER_NOTHING(0), //아무 플레이어도 해당되지 않는 경우
    PLAYER_1(1),
    PLAYER_2(2);

    private final int number;

    Player(int number) {
        this.number = number;
    }

    public static Player fromNumber(int number) {
        return Arrays.stream(values())
                .filter(p -> p.number == number)
                .findFirst()
                .orElseThrow(() ->new WebSocketBusinessException(INVALID_USER));
    }
}