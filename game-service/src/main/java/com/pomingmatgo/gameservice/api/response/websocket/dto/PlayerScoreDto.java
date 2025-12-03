package com.pomingmatgo.gameservice.api.response.websocket.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PlayerScoreDto {
    private int playerNumber;
    private int score;
}