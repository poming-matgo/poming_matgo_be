package com.pomingmatgo.gameservice.global;

import lombok.Getter;

@Getter
public class WebSocketResDto<T> {
    int playerId;
    private String status;
    private String message;
    private T data;

    public WebSocketResDto(int playerId, String status, String message) {
        this.playerId = playerId;
        this.status = status;
        this.message = message;
    }

    public WebSocketResDto(int playerId, String status, String message, T data) {
        this.playerId = playerId;
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public static <T> WebSocketResDto<T> of(int playerId, String status, String message) {
        return new WebSocketResDto<>(playerId, status, message, null);
    }

    public static <T> WebSocketResDto<T> of(int playerId, String status, String message, T data) {
        return new WebSocketResDto<>(playerId, status, message, data);
    }
}
