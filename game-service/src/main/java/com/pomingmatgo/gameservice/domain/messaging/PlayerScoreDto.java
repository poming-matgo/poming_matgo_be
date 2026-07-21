package com.pomingmatgo.gameservice.domain.messaging;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PlayerScoreDto {
    private int playerNumber;
    private int score;
}