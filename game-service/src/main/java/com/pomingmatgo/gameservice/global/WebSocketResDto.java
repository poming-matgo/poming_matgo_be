package com.pomingmatgo.gameservice.global;

import com.pomingmatgo.gameservice.domain.Player;
import lombok.Getter;

@Getter
public class WebSocketResDto<T> {
    private Player player;
    private String status;
    private String message;
    private T data;

    public WebSocketResDto(Player player, String status, String message) {
        this.player = player;
        this.status = status;
        this.message = message;
    }

    public WebSocketResDto(Player player, String status, String message, T data) {
        this.player = player;
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public static <T> WebSocketResDto<T> of(Player player, String status, String message) {
        return new WebSocketResDto<>(player, status, message, null);
    }

    public static <T> WebSocketResDto<T> of(Player player, String status, String message, T data) {
        return new WebSocketResDto<>(player, status, message, data);
    }
}
