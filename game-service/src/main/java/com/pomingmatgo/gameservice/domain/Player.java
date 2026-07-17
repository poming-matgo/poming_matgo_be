package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.Getter;

import java.util.Arrays;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_PLAYER;
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

    /** 상대 플레이어. PLAYER_NOTHING엔 상대가 없다 */
    public Player opponent() {
        return switch (this) {
            case PLAYER_1 -> PLAYER_2;
            case PLAYER_2 -> PLAYER_1;
            default -> throw new WebSocketBusinessException(INVALID_PLAYER);
        };
    }
}